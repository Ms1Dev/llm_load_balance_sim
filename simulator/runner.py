import asyncio
import json
import queue
import threading
import time

import aiohttp
import os

BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://bifrost:8080/v1")
API_KEY = os.environ.get("OPENAI_API_KEY", "mocked-openai-key-1")
MODEL = os.environ.get("OPENAI_MODEL", "openai/gpt-4o-mini")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
INTERVAL = 2.0  # seconds between requests

_running = False
_stop_event = threading.Event()
_thread = None
_stats = {"total": 0, "errors": 0, "total_latency_ms": 0}

_listeners: list[queue.Queue] = []
_listeners_lock = threading.Lock()


def is_running() -> bool:
    return _running


def subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=200)
    with _listeners_lock:
        _listeners.append(q)
    return q


def unsubscribe(q: queue.Queue):
    with _listeners_lock:
        try:
            _listeners.remove(q)
        except ValueError:
            pass


def _emit(event_type: str, data: dict):
    msg = {"type": event_type, "data": data}
    with _listeners_lock:
        for q in _listeners:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


def start():
    global _running, _thread, _stats
    if _running:
        return
    _stats = {"total": 0, "errors": 0, "total_latency_ms": 0}
    _stop_event.clear()
    _running = True
    _thread = threading.Thread(target=_thread_main, daemon=True)
    _thread.start()
    _emit("status", {"running": True})


def stop():
    global _running
    if not _running:
        return
    _running = False
    _stop_event.set()
    _emit("status", {"running": False})


def _thread_main():
    asyncio.run(_run_loop())


async def _run_loop():
    async with aiohttp.ClientSession() as session:
        while not _stop_event.is_set():
            t0 = time.monotonic()
            success = False
            error = ""
            input_tokens = 0
            output_tokens = 0

            try:
                async with session.post(
                    f"{BASE_URL}/chat/completions",
                    json={
                        "model": MODEL,
                        "messages": [{"role": "user", "content": "Say hello in one sentence."}],
                        "stream": True,
                        "stream_options": {"include_usage": True},
                    },
                    headers={"Authorization": f"Bearer {API_KEY}"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        error = "429 rate limited"
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
            _stats["total"] += 1
            if not success:
                _stats["errors"] += 1
            _stats["total_latency_ms"] += latency_ms

            _emit("result", {
                "latency_ms": latency_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "success": success,
                "error": error,
                "total": _stats["total"],
                "errors": _stats["errors"],
                "avg_latency_ms": round(_stats["total_latency_ms"] / _stats["total"]),
            })

            # Sleep for INTERVAL in small steps so stop is responsive
            deadline = time.monotonic() + INTERVAL
            while time.monotonic() < deadline and not _stop_event.is_set():
                await asyncio.sleep(0.1)
