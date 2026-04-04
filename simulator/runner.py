import asyncio
import json
import os
import random
import threading
import time

import aiohttp
import tiktoken

from utils.generate import generate_prompt


def _count_tokens(text: str) -> int:
    norm = MODEL.removeprefix("openai/")
    try:
        enc = tiktoken.encoding_for_model(norm)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))

BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://bifrost:8080/v1")
API_KEY = os.environ.get("OPENAI_API_KEY", "mocked-openai-key-1")
MODEL = os.environ.get("OPENAI_MODEL", "openai/gpt-4o-mini")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

USER_IDS = list(range(1, 101))

BASELINE_RPM = (2, 10)
NOISY_RPM    = (10, 60)

_running = False
_stop_event = threading.Event()
_thread = None
# Rolling window for dashboard aggregates: (monotonic_ts, success, latency_ms)
_stats_events: list[tuple[float, bool, int]] = []
_STATS_WINDOW_SEC = 60.0
_stats_lock = threading.Lock()


def is_running() -> bool:
    return _running


def start():
    global _running, _thread, _stats_events
    if _running:
        return
    _stats_events = []
    _stop_event.clear()
    _running = True
    _thread = threading.Thread(target=_thread_main, daemon=True)
    _thread.start()


def clear_stats():
    global _stats_events
    with _stats_lock:
        _stats_events = []


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
        prompt = generate_prompt(1)[0]
        input_tokens = _count_tokens(prompt)  # pre-count so 429s still show real token usage
        t0 = time.monotonic()
        success = False
        error = ""
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
                headers={"Authorization": f"Bearer {API_KEY}", "X-User-ID": str(user_id)},
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
                                input_tokens = usage.get("prompt_tokens", input_tokens)
                                output_tokens = usage.get("completion_tokens", 0)
                        except (json.JSONDecodeError, KeyError):
                            pass
                    success = True
        except Exception as exc:
            error = str(exc)[:120]

        latency_ms = round((time.monotonic() - t0) * 1000)
        t_done = time.monotonic()

        with _stats_lock:
            _stats_events.append((t_done, success, latency_ms))
            cutoff = t_done - _STATS_WINDOW_SEC
            _stats_events[:] = [e for e in _stats_events if e[0] >= cutoff]
            n = len(_stats_events)
            err_n = sum(1 for _, ok, _ in _stats_events if not ok)
            lat_sum = sum(lat for _, _, lat in _stats_events)
            snapshot = {
                "total": n,
                "errors": err_n,
                "avg_latency_ms": round(lat_sum / n),
            }

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
            "avg_latency_ms": snapshot["avg_latency_ms"],
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
