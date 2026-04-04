import json
import os
import time
import uuid

from utils.generate import generate_response
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://llm-mock:8001")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DEFAULT_RPM_LIMIT = int(os.environ.get("RATE_LIMIT_RPM", "30"))
DEFAULT_TPM_LIMIT = int(os.environ.get("RATE_LIMIT_TPM", "10000"))
DEFAULT_MAX_TOKENS = 500
SLIDING_WINDOW_SECONDS = 60.0

_config_cache: dict = {}
_config_updated_at: float = 0.0
_CONFIG_TTL = 5.0

# Atomic check-and-consume via Lua — prevents the race condition where concurrent
# requests all read below the limit before any of them write.
_SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local cost  = tonumber(ARGV[2])
local wstart = tonumber(ARGV[3])
local now    = tonumber(ARGV[4])
local member = ARGV[5]

redis.call('ZREMRANGEBYSCORE', key, 0, wstart)

local entries = redis.call('ZRANGE', key, 0, -1)
local current = 0
for _, m in ipairs(entries) do
    local c = string.match(m, ':(%d+)$')
    if c then current = current + tonumber(c) end
end

if current + cost > limit then
    return {0, math.max(0, limit - current)}
end

redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, 120)
return {1, math.max(0, limit - current - cost)}
"""


async def get_config(redis_client) -> dict:
    global _config_cache, _config_updated_at
    now = time.time()
    if now - _config_updated_at > _CONFIG_TTL:
        vals = await redis_client.mget('config:rpm_limit', 'config:tpm_limit')
        _config_cache = {
            'rpm_limit':  int(vals[0])   if vals[0] else DEFAULT_RPM_LIMIT,
            'tpm_limit':  int(vals[1])   if vals[1] else DEFAULT_TPM_LIMIT,
        }
        _config_updated_at = now
    return _config_cache


async def sliding_window_try_consume(
    redis_client: aioredis.Redis, key: str, limit: int, cost: int, window_seconds: float
) -> tuple[bool, int]:
    """Atomically check and consume. Returns (allowed, remaining)."""
    now = time.time()
    member = f"{uuid.uuid4()}:{cost}"
    result = await redis_client.eval(
        _SLIDING_WINDOW_SCRIPT, 1, key,
        limit, cost, now - window_seconds, now, member,
    )
    return bool(result[0]), int(result[1])


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def count_input_tokens(messages: list) -> int:
    return sum(
        estimate_tokens(msg.get("content", ""))
        for msg in messages
        if isinstance(msg.get("content"), str)
    )


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
    input_tokens = count_input_tokens(messages)
    max_tokens = body.get("max_tokens") or DEFAULT_MAX_TOKENS
    tokens_to_reserve = input_tokens + max_tokens

    redis_client: aioredis.Redis = request.app.state.redis
    cfg = await get_config(redis_client)
    rpm_limit = cfg['rpm_limit']
    tpm_limit = cfg['tpm_limit']

    # RPM: atomic check-and-consume. Every attempt counts, matching OpenAI behaviour.
    rpm_ok, rpm_remaining = await sliding_window_try_consume(
        redis_client, "rpm:sliding", rpm_limit, cost=1, window_seconds=SLIDING_WINDOW_SECONDS
    )
    await redis_client.set('status:rpm_remaining', rpm_remaining, ex=120)
    if not rpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {rpm_limit} RPM.",
                "type": "requests",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(max(1, int(SLIDING_WINDOW_SECONDS))),
                "x-ratelimit-limit-requests": str(rpm_limit),
                "x-ratelimit-remaining-requests": "0",
            },
        )

    # TPM: atomic check-and-consume. RPM slot already spent even if this fails.
    tpm_ok, tpm_remaining = await sliding_window_try_consume(
        redis_client, "tpm:sliding", tpm_limit, cost=tokens_to_reserve, window_seconds=SLIDING_WINDOW_SECONDS
    )
    await redis_client.set('status:tpm_remaining', tpm_remaining, ex=120)
    if not tpm_ok:
        return JSONResponse(
            status_code=429,
            content={"error": {
                "message": f"Rate limit exceeded: {tpm_limit} TPM.",
                "type": "tokens",
                "code": "rate_limit_exceeded",
            }},
            headers={
                "Retry-After": str(max(1, int(SLIDING_WINDOW_SECONDS))),
                "x-ratelimit-limit-tokens": str(tpm_limit),
                "x-ratelimit-remaining-tokens": "0",
            },
        )

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
        output_tokens = estimate_tokens(content)
        print(f"[limiter] rpm_remaining={rpm_remaining} tpm_remaining={tpm_remaining} input={input_tokens} output={output_tokens} reserved={tokens_to_reserve}", flush=True)
        return JSONResponse(content=data, status_code=response.status_code, headers=rl_headers)

    async def stream_response():
        text, output_tokens = generate_response(input_tokens)
        resp_id = f"chatcmpl-{uuid.uuid4().hex}"

        yield "data: " + json.dumps({
            "id": resp_id,
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}],
        }) + "\n"

        yield "data: " + json.dumps({
            "choices": [],
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
            },
        }) + "\n"
        yield "data: [DONE]\n"
        print(f"[limiter] rpm_remaining={rpm_remaining} tpm_remaining={tpm_remaining} input={input_tokens} output={output_tokens} reserved={tokens_to_reserve}", flush=True)

    return StreamingResponse(stream_response(), media_type="text/event-stream", headers=rl_headers)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/models")
async def models():
    return {"data": [{"id": "gpt-4o-mini"}, {"id": "gpt-4o"}]}
