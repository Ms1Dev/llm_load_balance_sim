import json
import os
import random
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect

BIFROST_URL      = os.environ.get("BIFROST_URL",        "http://bifrost:8080/v1")
REDIS_URL        = os.environ.get("REDIS_URL",          "redis://redis:6379")
COST_PER_1K_INPUT  = float(os.environ.get("COST_PER_1K_INPUT",  "0.00015"))
COST_PER_1K_OUTPUT = float(os.environ.get("COST_PER_1K_OUTPUT", "0.00060"))

MAX_RETRIES: int = 5
BASE_DELAY: float = 1.0
MAX_DELAY: float = 60.0
EXPONENTIAL_BASE: float = 2.0
JITTER: bool = True

_SKIP_HEADERS = {"host", "content-length", "transfer-encoding"}


# ── Backoff ───────────────────────────────────────────────────────────────

def _parse_retry_after(headers: httpx.Headers) -> float | None:
    raw = headers.get("retry-after")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _calculate_delay(attempt: int) -> float:
    delay = BASE_DELAY * (EXPONENTIAL_BASE ** attempt)
    delay = min(delay, MAX_DELAY)
    if JITTER:
        delay = delay * (0.5 + random.random())  # 0.5× – 1.5× multiplier
    return delay


async def _sleep_backoff(attempt: int, retry_after_s: float | None = None) -> None:
    delay = _calculate_delay(attempt)
    if retry_after_s is not None:
        delay = max(delay, retry_after_s)
    await asyncio.sleep(delay)


# ── Cost tracking ─────────────────────────────────────────────────────────

async def _record_usage(redis: aioredis.Redis, user_id: str,
                        input_tokens: int, output_tokens: int) -> None:
    cost = (input_tokens  / 1000 * COST_PER_1K_INPUT +
            output_tokens / 1000 * COST_PER_1K_OUTPUT)
    pipe = redis.pipeline()
    pipe.hincrbyfloat(f"usage:{user_id}:tokens", "input",  input_tokens)
    pipe.hincrbyfloat(f"usage:{user_id}:tokens", "output", output_tokens)
    pipe.hincrbyfloat(f"usage:{user_id}:cost",   "total",  cost)
    pipe.hincrbyfloat("usage:global:cost",        "total",  cost)
    await pipe.execute()
    await redis.publish("events:usage", json.dumps({
        "user_id":       user_id,
        "input_tokens":  input_tokens,
        "output_tokens": output_tokens,
        "cost_usd":      round(cost, 6),
        "ts":            datetime.now().isoformat(),
    }))


def _parse_usage_from_stream(chunks: list[bytes]) -> tuple[int, int]:
    input_tokens = output_tokens = 0
    for raw in chunks:
        line = raw.decode(errors="replace").strip()
        if not line.startswith("data: "):
            continue
        payload = line[6:]
        if payload == "[DONE]":
            continue
        try:
            chunk = json.loads(payload)
            usage = chunk.get("usage")
            if usage:
                input_tokens  = usage.get("prompt_tokens",     0)
                output_tokens = usage.get("completion_tokens", 0)
        except (json.JSONDecodeError, KeyError):
            pass
    return input_tokens, output_tokens


# ── App ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(base_url=BIFROST_URL, timeout=120.0)
    app.state.redis  = aioredis.from_url(REDIS_URL)
    yield
    await app.state.client.aclose()
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


async def _get_strategies(redis: aioredis.Redis) -> set[str]:
    members = await redis.smembers("config:strategies")
    return {m.decode() for m in members}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    try:
        body = await request.body()
    except ClientDisconnect:
        return JSONResponse(status_code=499, content={"detail": "client disconnected"})

    user_id = request.headers.get("x-user-id", "anonymous")
    redis: aioredis.Redis = request.app.state.redis

    strategies = await _get_strategies(redis)
    use_backoff = "backoff" in strategies

    forward_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _SKIP_HEADERS
    }

    client: httpx.AsyncClient = request.app.state.client
    last_response = None
    max_attempts  = MAX_RETRIES + 1 if use_backoff else 1

    for attempt in range(max_attempts):
        req = client.build_request(
            "POST", "/chat/completions",
            content=body,
            headers=forward_headers,
        )
        upstream_resp = await client.send(req, stream=True)

        # Success — break out and stream back
        if upstream_resp.status_code == 200:
            break

        # Retryable error — read body, maybe retry
        if upstream_resp.status_code in (429,) or upstream_resp.status_code >= 500:
            error_body = await upstream_resp.aread()
            await upstream_resp.aclose()
            last_response = (upstream_resp.status_code, error_body)

            # Publish retry event so dashboard can show backoff activity
            await redis.publish("events:backoff", json.dumps({
                "user_id": user_id,
                "attempt": attempt,
                "status":  upstream_resp.status_code,
                "ts":      datetime.now().isoformat(),
            }))

            if attempt >= max_attempts - 1:
                break

            retry_after = _parse_retry_after(upstream_resp.headers)
            await _sleep_backoff(attempt, retry_after_s=retry_after)
            continue

        # Non-retryable error (4xx etc) — return immediately
        error_body = await upstream_resp.aread()
        await upstream_resp.aclose()
        return JSONResponse(
            content=json.loads(error_body),
            status_code=upstream_resp.status_code,
        )

    # All attempts exhausted without success
    if last_response and upstream_resp.status_code != 200:
        status_code, error_body = last_response
        return JSONResponse(
            content=json.loads(error_body),
            status_code=status_code,
        )

    # Stream successful response back
    response_headers = {
        k: v for k, v in upstream_resp.headers.items()
        if k.lower() not in {"content-length", "transfer-encoding", "content-encoding"}
    }

    accumulated: list[bytes] = []

    async def proxy_stream():
        try:
            async for chunk in upstream_resp.aiter_raw():
                accumulated.append(chunk)
                yield chunk
        finally:
            await upstream_resp.aclose()
            input_tokens, output_tokens = _parse_usage_from_stream(accumulated)
            if input_tokens or output_tokens:
                await _record_usage(redis, user_id, input_tokens, output_tokens)

    return StreamingResponse(
        proxy_stream(),
        status_code=upstream_resp.status_code,
        media_type=upstream_resp.headers.get("content-type", "text/event-stream"),
        headers=response_headers,
    )


# ── Config endpoints (called by dashboard UI) ─────────────────────────────

@app.post("/config/backoff")
async def set_backoff(request: Request):
    body    = await request.json()
    enabled = "1" if body.get("enabled") else "0"
    await request.app.state.redis.set("config:backoff_enabled", enabled)
    return {"backoff_enabled": enabled == "1"}


@app.get("/config/backoff")
async def get_backoff(request: Request):
    val = await request.app.state.redis.get("config:backoff_enabled")
    return {"backoff_enabled": val and val.decode() == "1"}


# ── Usage endpoints ───────────────────────────────────────────────────────

@app.get("/usage/{user_id}")
async def get_user_usage(user_id: str, request: Request):
    redis: aioredis.Redis = request.app.state.redis
    tokens = await redis.hmget(f"usage:{user_id}:tokens", "input", "output")
    cost   = await redis.hget(f"usage:{user_id}:cost", "total")
    return {
        "user_id":       user_id,
        "input_tokens":  int(float(tokens[0] or 0)),
        "output_tokens": int(float(tokens[1] or 0)),
        "cost_usd":      round(float(cost or 0), 6),
    }


@app.get("/usage")
async def get_all_usage(request: Request):
    redis: aioredis.Redis = request.app.state.redis
    cost = await redis.hget("usage:global:cost", "total")
    return {"total_cost_usd": round(float(cost or 0), 6)}


@app.get("/health")
async def health():
    return {"status": "ok"}