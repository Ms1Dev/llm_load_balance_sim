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
RPM_LIMIT = int(os.environ.get("RATE_LIMIT_RPM", "30"))
TPM_LIMIT = int(os.environ.get("RATE_LIMIT_TPM", "10000"))

# If a request doesn't specify max_tokens, assume this many output tokens when reserving
DEFAULT_MAX_TOKENS = 500

# Compress the rate limit window by 4x so a 30s test spans two full window cycles
TIME_SCALE = 4
WINDOW_SECONDS = 60.0 / TIME_SCALE


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
    redis_client: aioredis.Redis, key: str, limit: int, cost: int
) -> tuple[bool, int, int, list]:
    """
    Check a sliding 60-second window without consuming capacity.
    Returns (allowed, remaining, retry_after_seconds, entries).
    """
    now = time.time()
    window_start = now - WINDOW_SECONDS

    async with redis_client.pipeline(transaction=False) as pipe:
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zrange(key, 0, -1, withscores=True)
        results = await pipe.execute()

    entries = results[1]  # [(member, score), ...] oldest first
    current = sum(int(member.split(":")[1]) for member, _score in entries)
    remaining = limit - current

    if current + cost > limit:
        freed = 0
        retry_after = 60
        for member, score in entries:
            freed += int(member.split(":")[1])
            if current - freed + cost <= limit:
                retry_after = max(1, int(60 - (now - score)) + 1)
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

    rpm_ok, rpm_remaining, rpm_retry, _ = await sliding_window_check(
        redis_client, "rpm:sliding", RPM_LIMIT, cost=1
    )
    if not rpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {RPM_LIMIT} RPM.",
                "type": "requests",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(rpm_retry),
                "x-ratelimit-limit-requests": str(RPM_LIMIT),
                "x-ratelimit-remaining-requests": "0",
            },
        )

    tpm_ok, tpm_remaining, tpm_retry, _ = await sliding_window_check(
        redis_client, "tpm:sliding", TPM_LIMIT, cost=tokens_to_reserve
    )
    if not tpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {TPM_LIMIT} TPM.",
                "type": "tokens",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(tpm_retry),
                "x-ratelimit-limit-tokens": str(TPM_LIMIT),
                "x-ratelimit-remaining-tokens": "0",
            },
        )

    await sliding_window_consume(redis_client, "rpm:sliding", cost=1)
    await sliding_window_consume(redis_client, "tpm:sliding", cost=tokens_to_reserve)

    rl_headers = {
        "x-ratelimit-limit-requests": str(RPM_LIMIT),
        "x-ratelimit-remaining-requests": str(rpm_remaining),
        "x-ratelimit-limit-tokens": str(TPM_LIMIT),
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
