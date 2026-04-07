import asyncio
import json
import os
import random
import threading
import time

import aiohttp
import math
import tiktoken
from enum import Enum
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

USER_IDS = list(range(1, 51))

DEFAULT_BASELINE_RPM = 6
SPAMMER_RPM  = 200
BURSTY_RPM   = 120
VARIABILITY  = 5

# Bursty cycle: burst at BURSTY_RPM for a short window, then go quiet.
# Expected average: 120 × 8.5s / (8.5 + 91.5)s ≈ 10 RPM
BURSTY_BURST_MIN =  5.0
BURSTY_BURST_MAX = 12.0
BURSTY_PAUSE_MIN = 75.0
BURSTY_PAUSE_MAX = 108.0

class Usage(Enum):
    CONISTENT = "consistent"
    SINE_WAVE = "sine_wave"
    BURSTY = "bursty"

USAGE_MULTIPLIER = Usage.SINE_WAVE

_running = False
_stop_event = threading.Event()
_thread = None
# Rolling window for dashboard aggregates: (monotonic_ts, success, latency_ms)
_stats_events: list[tuple[float, bool, int]] = []
_STATS_WINDOW_SEC = 60.0
_stats_lock = threading.Lock()



def get_usage_multiplier(usage: Usage) -> tuple[float, float]:
    if usage == Usage.CONISTENT:
        return 1
    if usage == Usage.SINE_WAVE:
        # Generate a sine multiplier between 0.5 and 1.5.
        # The value completes one full cycle every minute.
        t = time.time()  # current time in seconds
        # (t % 60) goes from 0 to 59 within each minute
        phase = (t % 60) / 60.0  # 0 to <1 over a minute
        # Sine in [0, 2pi] over a minute
        sine_value = 0.5 * (math.sin(2 * math.pi * phase) + 1) + 0.5  # Range: 0.5—1.5
        return sine_value

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


async def _get_user_api_key(redis_client, user_id: int) -> str:
    key = await redis_client.get(f'config:vkey:{user_id}')
    return key.decode() if key else API_KEY


async def _get_baseline_rpm(redis_client) -> float:
    val = await redis_client.get('config:normal_user_rpm')
    return float(val) if val else DEFAULT_BASELINE_RPM


async def _get_user_mode(redis_client, user_id: int) -> str:
    """Return 'spammer', 'bursty', or 'normal'."""
    pipe = redis_client.pipeline(transaction=False)
    pipe.sismember('config:spammer_users', str(user_id))
    pipe.sismember('config:bursty_users',  str(user_id))
    is_spammer, is_bursty = await pipe.execute()
    if is_spammer:
        return 'spammer'
    if is_bursty:
        return 'bursty'
    return 'normal'


async def _user_loop(session: aiohttp.ClientSession, user_id: int, redis_client):
    # Local bursty state machine — no Redis state needed
    prev_mode       = 'normal'
    bursty_bursting = False
    bursty_phase_end = 0.0

    await asyncio.sleep(random.uniform(0, 20.0))   # spread initial requests across 20s

    while not _stop_event.is_set():
        mode = await _get_user_mode(redis_client, user_id)

        # ── Bursty phase management ──────────────────────────────────────
        if mode == 'bursty':
            now = time.monotonic()
            # Entering bursty mode fresh → start in burst immediately
            if prev_mode != 'bursty':
                bursty_bursting  = True
                bursty_phase_end = now + random.uniform(BURSTY_BURST_MIN, BURSTY_BURST_MAX)
            elif now >= bursty_phase_end:
                bursty_bursting  = not bursty_bursting
                duration = (random.uniform(BURSTY_BURST_MIN, BURSTY_BURST_MAX)
                            if bursty_bursting else
                            random.uniform(BURSTY_PAUSE_MIN, BURSTY_PAUSE_MAX))
                bursty_phase_end = now + duration

            if not bursty_bursting:
                # Pause phase — sleep until phase end, wake early if mode changes
                while time.monotonic() < bursty_phase_end and not _stop_event.is_set():
                    await asyncio.sleep(min(1.0, bursty_phase_end - time.monotonic()))
                    new_mode = await _get_user_mode(redis_client, user_id)
                    if new_mode != 'bursty':
                        mode = new_mode
                        break
                prev_mode = mode
                continue   # re-enter loop to recheck phase / handle mode change
        else:
            bursty_bursting  = False
            bursty_phase_end = 0.0

        prev_mode = mode

        # ── Determine RPM for this request ──────────────────────────────
        if mode == 'spammer':
            rpm = SPAMMER_RPM
        elif mode == 'bursty':
            rpm = BURSTY_RPM
        else:
            baseline = await _get_baseline_rpm(redis_client)
            rpm = _bell(max(1, baseline - VARIABILITY), baseline + VARIABILITY) * get_usage_multiplier(USAGE_MULTIPLIER)
        prompt = generate_prompt(1)[0]
        input_tokens = _count_tokens(prompt)  # pre-count so 429s still show real token usage
        t0 = time.monotonic()
        success = False
        error = ""
        output_tokens = 0
        status_code = 0
        rpm_remaining = -1
        tpm_remaining = -1

        await _emit("request_start", {"user_id": user_id})

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
                headers={"Authorization": f"Bearer {await _get_user_api_key(redis_client, user_id)}", "X-User-ID": str(user_id)},
                timeout=aiohttp.ClientTimeout(total=120),
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

        # Poisson inter-arrival: exponentially distributed with the target rate as the mean.
        # Cap at 3× mean so an unlucky draw doesn't stall a user tile for ages.
        mean_interval = 60.0 / rpm
        interval = min(mean_interval * 3, random.expovariate(rpm / 60.0))
        deadline = time.monotonic() + interval
        while time.monotonic() < deadline and not _stop_event.is_set():
            await asyncio.sleep(min(1.0, deadline - time.monotonic()))
            # Normal users: poll for mode escalation so they switch immediately
            if mode == 'normal' and deadline - time.monotonic() > 0.5:
                new_mode = await _get_user_mode(redis_client, user_id)
                if new_mode != 'normal':
                    break


async def _run_loop():
    import redis.asyncio as aioredis
    redis_client = aioredis.from_url(REDIS_URL)
    await _emit("status", {"running": True})
    try:
        # Many user loops share one session; HTTP/1.1 needs enough parallel connections
        # to the backend or requests (and their backoffs) serialize on the wire.
        connector = aiohttp.TCPConnector(limit=256, limit_per_host=256)
        async with aiohttp.ClientSession(connector=connector) as session:
            await asyncio.gather(*[
                _user_loop(session, uid, redis_client) for uid in USER_IDS
            ])
    finally:
        await redis_client.aclose()
    await _emit("status", {"running": False})
