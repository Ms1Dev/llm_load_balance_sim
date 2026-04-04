import asyncio
import os
import time
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

BIFROST_URL = os.environ.get("BIFROST_URL", "http://bifrost:8080/v1")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DEFAULT_RPM = 30

_SKIP_HEADERS = {"host", "content-length", "transfer-encoding"}


class TokenBucket:
    """
    Leaky-bucket rate vLLM. Callers await acquire() which blocks until a
    token is available. This turns 429s into latency — users wait instead of
    getting errors.

    Rate is read from Redis (config:rpm_limit) every 5 seconds so live UI
    changes take effect without a restart.
    """

    def __init__(self, rpm):
        self._rpm = max(1, rpm)
        self._tokens = 0.0
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def set_rate(self, rpm: int):
        new_rpm = max(1, rpm)
        if new_rpm != self._rpm:
            self._rpm = new_rpm
            self._tokens = 0.0  # reset on rate change so new limit bites immediately

    def track(self):
        """Consume a token without blocking — called in passthrough mode so the
        bucket always reflects real load. When throttle is toggled on, it's
        already calibrated and kicks in immediately.

        Tokens are clamped at 0 so debt never accumulates: if load is high the
        bucket sits at 0, and throttle can start serving within one refill period
        (60 / rpm seconds) rather than waiting to climb out of a large deficit."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._rpm),
            self._tokens + elapsed * (self._rpm / 60.0),
        ) - 1.0
        self._tokens = max(self._tokens, 0.0)
        self._last_refill = now

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    float(self._rpm),
                    self._tokens + elapsed * (self._rpm / 60.0),
                )
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / (self._rpm / 60.0)
            await asyncio.sleep(wait)


async def _sync_rate(redis_client: aioredis.Redis, bucket: TokenBucket):
    """Background task: keep the token bucket in sync with config:rpm_limit."""
    while True:
        try:
            val = await redis_client.get("config:rpm_limit")
            if val:
                bucket.set_rate(int(val))
        except Exception:
            pass
        await asyncio.sleep(5.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(base_url=BIFROST_URL, timeout=120.0)
    app.state.redis = aioredis.from_url(REDIS_URL)
    initial_rpm = await app.state.redis.get("config:rpm_limit")
    app.state.bucket = TokenBucket(int(initial_rpm) if initial_rpm else DEFAULT_RPM)
    app.state.throttle_active = False
    app.state.sync_task = asyncio.create_task(
        _sync_rate(app.state.redis, app.state.bucket)
    )
    yield
    app.state.sync_task.cancel()
    await app.state.client.aclose()
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


async def _get_strategies(redis_client: aioredis.Redis) -> set[str]:
    members = await redis_client.smembers("config:strategies")
    return {m.decode() for m in members}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    # Buffer body immediately — if we wait until after acquire() the client
    # may have disconnected and the stream is gone, causing ClientDisconnect.
    try:
        body = await request.body()
    except ClientDisconnect:
        return JSONResponse(status_code=499, content={"detail": "client disconnected"})

    redis_client: aioredis.Redis = request.app.state.redis
    strategies = await _get_strategies(redis_client)

    bucket: TokenBucket = request.app.state.bucket
    if "throttle" in strategies:
        if not request.app.state.throttle_active:
            # Throttle was just enabled — drain pre-accumulated tokens so the
            # rate limit bites on this request rather than after a long burst.
            request.app.state.throttle_active = True
            bucket._tokens = 0.0
        await bucket.acquire()
    else:
        request.app.state.throttle_active = False
        # Shadow-track in passthrough so the bucket reflects real load.
        # Toggling throttle on will bite immediately instead of after a warm-up.
        bucket.track()
    forward_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _SKIP_HEADERS
    }

    client: httpx.AsyncClient = request.app.state.client
    req = client.build_request(
        "POST", "/chat/completions",
        content=body,
        headers=forward_headers,
    )
    upstream_resp = await client.send(req, stream=True)

    response_headers = {
        k: v for k, v in upstream_resp.headers.items()
        if k.lower() not in {"content-length", "transfer-encoding", "content-encoding"}
    }

    # Bifrost strips x-ratelimit-* headers — inject them from Redis where the
    # vLLM writes its latest remaining counts after each request.
    rl_vals = await redis_client.mget("status:rpm_remaining", "status:tpm_remaining")
    if rl_vals[0] is not None:
        response_headers["x-ratelimit-remaining-requests"] = rl_vals[0].decode()
    if rl_vals[1] is not None:
        response_headers["x-ratelimit-remaining-tokens"] = rl_vals[1].decode()

    if upstream_resp.status_code != 200:
        error_body = await upstream_resp.aread()
        await upstream_resp.aclose()
        return JSONResponse(
            content=__import__("json").loads(error_body),
            status_code=upstream_resp.status_code,
            headers=response_headers,
        )

    async def proxy_stream():
        try:
            async for chunk in upstream_resp.aiter_raw():
                yield chunk
        finally:
            await upstream_resp.aclose()

    return StreamingResponse(
        proxy_stream(),
        status_code=upstream_resp.status_code,
        media_type=upstream_resp.headers.get("content-type", "text/event-stream"),
        headers=response_headers,
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status(request: Request):
    redis_client: aioredis.Redis = request.app.state.redis
    strategies = await _get_strategies(redis_client)
    bucket: TokenBucket = request.app.state.bucket
    return {
        "active_strategies": sorted(strategies),
        "token_bucket": {
            "rate_rpm": bucket._rpm,
            "tokens_available": round(bucket._tokens, 2),
        },
    }
