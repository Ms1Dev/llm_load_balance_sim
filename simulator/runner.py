import asyncio
import json
import os
import random
import threading
import time

import aiohttp

BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://bifrost:8080/v1")
API_KEY = os.environ.get("OPENAI_API_KEY", "mocked-openai-key-1")
MODEL = os.environ.get("OPENAI_MODEL", "openai/gpt-4o-mini")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

USER_IDS = list(range(1, 101))

BASELINE_RPM = (5,20)
BASELINE_PROMPT_WORDS = (15,60)

NOISY_RPM = (15,60)
NOISY_PROMPT_WORDS = (60,120)

_LOREM = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
    "incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud "
    "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute "
    "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
    "pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia "
    "deserunt mollit anim id est laborum sed ut perspiciatis unde omnis iste natus error "
    "sit voluptatem accusantium doloremque laudantium totam rem aperiam eaque ipsa quae "
    "ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo"
).split()

_running = False
_stop_event = threading.Event()
_thread = None
_stats = {"total": 0, "errors": 0, "total_latency_ms": 0}
_stats_lock = threading.Lock()


def is_running() -> bool:
    return _running


def start():
    global _running, _thread, _stats
    if _running:
        return
    _stats = {"total": 0, "errors": 0, "total_latency_ms": 0}
    _stop_event.clear()
    _running = True
    _thread = threading.Thread(target=_thread_main, daemon=True)
    _thread.start()


def clear_stats():
    global _stats
    with _stats_lock:
        _stats = {"total": 0, "errors": 0, "total_latency_ms": 0}


def stop():
    global _running
    if not _running:
        return
    _running = False
    _stop_event.set()


def _bell(low: float, high: float) -> float:
    mean = (low + high) / 2
    std = (high - low) / 6
    return max(low, min(high, random.gauss(mean, std)))


def _random_prompt(target_words: int) -> str:
    low = max(5, int(target_words * 0.5))
    high = min(len(_LOREM) - 1, int(target_words * 1.5))
    length = random.randint(low, high)
    start = random.randint(0, len(_LOREM) - length)
    words = _LOREM[start:start + length]
    words[0] = words[0].capitalize()
    return " ".join(words) + "?"


def _thread_main():
    asyncio.run(_run_loop())


async def _emit(event_type: str, data: dict):
    from channels.layers import get_channel_layer
    channel_layer = get_channel_layer()
    if channel_layer is None:
        return
    await channel_layer.group_send("simulator", {
        "type": "simulator.event",
        "event_type": event_type,
        "data": data,
    })


async def _get_noisy_ids(redis_client) -> set[int]:
    members = await redis_client.smembers('config:noisy_users')
    return {int(m) for m in members}


async def _user_loop(session: aiohttp.ClientSession, user_id: int, redis_client):
    noisy_ids = await _get_noisy_ids(redis_client)
    is_noisy = user_id in noisy_ids
    interval = 60.0 / _bell(*(NOISY_RPM if is_noisy else BASELINE_RPM))
    await asyncio.sleep(random.uniform(0, interval))

    while not _stop_event.is_set():
        noisy_ids = await _get_noisy_ids(redis_client)
        is_noisy = user_id in noisy_ids
        rpm = _bell(*(NOISY_RPM if is_noisy else BASELINE_RPM))
        prompt_words = random.randint(*(NOISY_PROMPT_WORDS if is_noisy else BASELINE_PROMPT_WORDS))

        prompt = _random_prompt(prompt_words)
        t0 = time.monotonic()
        success = False
        error = ""
        input_tokens = 0
        output_tokens = 0
        status_code = 0
        rpm_remaining = -1
        tpm_remaining = -1

        await _emit("request_start", {"user_id": user_id, "noisy": is_noisy})

        try:
            async with session.post(
                f"{BASE_URL}/chat/completions",
                json={
                    "model": MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": True,
                    "stream_options": {"include_usage": True},
                    "temperature": 1,
                    "n": 1,
                },
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                status_code = resp.status
                rpm_remaining = int(resp.headers.get("x-ratelimit-remaining-requests", -1))
                tpm_remaining = int(resp.headers.get("x-ratelimit-remaining-tokens", -1))
                if resp.status == 429:
                    body = await resp.json()
                    error_type = body.get("error", {}).get("type", "")
                    error = "429:tokens" if error_type == "tokens" else "429"
                else:
                    resp.raise_for_status()
                    async for raw_line in resp.content:
                        line = raw_line.decode().strip()
                        if not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload == "[DONE]":
                            break
                        try:
                            chunk = json.loads(payload)
                            usage = chunk.get("usage")
                            if usage:
                                input_tokens = usage.get("prompt_tokens", 0)
                                output_tokens = usage.get("completion_tokens", 0)
                        except (json.JSONDecodeError, KeyError):
                            pass
                    success = True
        except Exception as exc:
            error = str(exc)[:120]

        latency_ms = round((time.monotonic() - t0) * 1000)

        with _stats_lock:
            _stats["total"] += 1
            if not success:
                _stats["errors"] += 1
            _stats["total_latency_ms"] += latency_ms
            snapshot = dict(_stats)

        await _emit("result", {
            "user_id": user_id,
            "noisy": is_noisy,
            "status_code": status_code,
            "latency_ms": latency_ms,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "success": success,
            "error": error,
            "rpm_remaining": rpm_remaining,
            "tpm_remaining": tpm_remaining,
            "total": snapshot["total"],
            "errors": snapshot["errors"],
            "avg_latency_ms": round(snapshot["total_latency_ms"] / snapshot["total"]),
        })

        interval = 60.0 / rpm
        deadline = time.monotonic() + interval
        while time.monotonic() < deadline and not _stop_event.is_set():
            await asyncio.sleep(0.1)


async def _run_loop():
    import redis.asyncio as aioredis
    redis_client = aioredis.from_url(REDIS_URL)
    await _emit("status", {"running": True})
    try:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                _user_loop(session, uid, redis_client) for uid in USER_IDS
            ])
    finally:
        await redis_client.aclose()
    await _emit("status", {"running": False})
