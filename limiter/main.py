import json
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://llm-mock:8001")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DEFAULT_RPM_LIMIT = int(os.environ.get("RATE_LIMIT_RPM", "30"))
DEFAULT_TPM_LIMIT = int(os.environ.get("RATE_LIMIT_TPM", "10000"))

# If a request doesn't specify max_tokens, assume this many output tokens when reserving
DEFAULT_MAX_TOKENS = 500

# Config is read from Redis (written by the Django Config model) with a short TTL cache
DEFAULT_TIME_SCALE = 4
_config_cache: dict = {}
_config_updated_at: float = 0.0
_CONFIG_TTL = 5.0


async def get_config(redis_client) -> dict:
    global _config_cache, _config_updated_at
    now = time.time()
    if now - _config_updated_at > _CONFIG_TTL:
        vals = await redis_client.mget('config:time_scale', 'config:rpm_limit', 'config:tpm_limit')
        _config_cache = {
            'time_scale': float(vals[0]) if vals[0] else DEFAULT_TIME_SCALE,
            'rpm_limit':  int(vals[1])   if vals[1] else DEFAULT_RPM_LIMIT,
            'tpm_limit':  int(vals[2])   if vals[2] else DEFAULT_TPM_LIMIT,
        }
        _config_updated_at = now
    return _config_cache


import tiktoken

def estimate_tokens(model: str, text: str) -> int:
    """Estimate tokens using tiktoken encoding for a reasonable default model."""
    enc = tiktoken.encoding_for_model(model)
    return max(1, len(enc.encode(text)))



def count_input_tokens(model: str, messages: list) -> int:
    total = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total += estimate_tokens(model, content)
    return total


async def sliding_window_check(
    redis_client: aioredis.Redis, key: str, limit: int, cost: int, window_seconds: float
) -> tuple[bool, int, int, list]:
    """
    Check a sliding window without consuming capacity.
    Returns (allowed, remaining, retry_after_seconds, entries).
    """
    now = time.time()
    window_start = now - window_seconds

    async with redis_client.pipeline(transaction=False) as pipe:
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zrange(key, 0, -1, withscores=True)
        results = await pipe.execute()

    entries = results[1]  # [(member, score), ...] oldest first
    current = sum(int(member.split(":")[1]) for member, _score in entries)
    remaining = limit - current

    if current + cost > limit:
        freed = 0
        retry_after = window_seconds
        for member, score in entries:
            freed += int(member.split(":")[1])
            if current - freed + cost <= limit:
                retry_after = max(1, int(window_seconds - (now - score)) + 1)
                break
        return False, max(0, remaining), retry_after, entries

    return True, remaining - cost, 0, entries


async def sliding_window_consume(
    redis_client: aioredis.Redis, key: str, cost: int
) -> None:
    """Record a consumed slot after a successful check."""
    member = f"{uuid.uuid4()}:{cost}"
    await redis_client.zadd(key, {member: time.time()})
    await redis_client.expire(key, 120)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(base_url=UPSTREAM_URL, timeout=120.0)
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    await app.state.client.aclose()
    await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    stream = body.get("stream", False)
    messages = body.get("messages", [])
    model = body.get("model", "gpt-4o-mini")
    input_tokens = count_input_tokens(model, messages)
    max_tokens = body.get("max_tokens") or DEFAULT_MAX_TOKENS
    tokens_to_reserve = input_tokens + max_tokens

    redis_client: aioredis.Redis = request.app.state.redis
    cfg = await get_config(redis_client)
    window_seconds = 60.0 / cfg['time_scale']
    rpm_limit = cfg['rpm_limit']
    tpm_limit = cfg['tpm_limit']

    rpm_ok, rpm_remaining, rpm_retry, _ = await sliding_window_check(
        redis_client, "rpm:sliding", rpm_limit, cost=1, window_seconds=window_seconds
    )
    if not rpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {rpm_limit} RPM.",
                "type": "requests",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(rpm_retry),
                "x-ratelimit-limit-requests": str(rpm_limit),
                "x-ratelimit-remaining-requests": "0",
            },
        )

    tpm_ok, tpm_remaining, tpm_retry, _ = await sliding_window_check(
        redis_client, "tpm:sliding", tpm_limit, cost=tokens_to_reserve, window_seconds=window_seconds
    )
    if not tpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {tpm_limit} TPM.",
                "type": "tokens",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(tpm_retry),
                "x-ratelimit-limit-tokens": str(tpm_limit),
                "x-ratelimit-remaining-tokens": "0",
            },
        )

    await sliding_window_consume(redis_client, "rpm:sliding", cost=1)
    await sliding_window_consume(redis_client, "tpm:sliding", cost=tokens_to_reserve)

    rl_headers = {
        "x-ratelimit-limit-requests": str(rpm_limit),
        "x-ratelimit-remaining-requests": str(rpm_remaining),
        "x-ratelimit-limit-tokens": str(tpm_limit),
        "x-ratelimit-remaining-tokens": str(tpm_remaining),
    }

    upstream_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding")
    }
    client: httpx.AsyncClient = request.app.state.client

    if not stream:
        response = await client.post("/v1/chat/completions", json=body, headers=upstream_headers)
        data = response.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        output_tokens = estimate_tokens(model, content)
        print(f"[limiter] rpm_remaining={rpm_remaining} tpm_remaining={tpm_remaining} input={input_tokens} output={output_tokens} reserved={tokens_to_reserve}", flush=True)
        return JSONResponse(content=data, status_code=response.status_code, headers=rl_headers)

    async def stream_response():
        output_tokens = 0
        try:
            async with client.stream("POST", "/v1/chat/completions", json=body, headers=upstream_headers) as response:
                async for line in response.aiter_lines():
                    if line == "data: [DONE]":
                        usage_chunk = json.dumps({
                            "choices": [],
                            "usage": {
                                "prompt_tokens": input_tokens,
                                "completion_tokens": output_tokens,
                                "total_tokens": input_tokens + output_tokens,
                            },
                        })
                        yield f"data: {usage_chunk}\n"
                    elif line.startswith("data: "):
                        try:
                            chunk = json.loads(line[6:])
                            content = (chunk.get("choices") or [{}])[0].get("delta", {}).get("content") or ""
                            output_tokens += estimate_tokens(model, content)
                        except (json.JSONDecodeError, IndexError, KeyError):
                            pass
                    yield line + "\n" if line else "\n"
        finally:
            print(f"[limiter] rpm_remaining={rpm_remaining} tpm_remaining={tpm_remaining} input={input_tokens} output={output_tokens} reserved={tokens_to_reserve}", flush=True)

    return StreamingResponse(stream_response(), media_type="text/event-stream", headers=rl_headers)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/models")
async def models():
    return {"data": [{"id": "gpt-4o-mini"}, {"id": "gpt-4o"}]}