import json
import os
from datetime import datetime

import redis.asyncio as aioredis

COST_PER_1M_INPUT = float(os.environ.get("COST_PER_1M_INPUT", "0.75"))
COST_PER_1M_OUTPUT = float(os.environ.get("COST_PER_1M_OUTPUT", "4.50"))


async def record_usage(
    redis: aioredis.Redis, user_id: str, input_tokens: int, output_tokens: int
) -> None:
    cost = (
        input_tokens / 1_000_000 * COST_PER_1M_INPUT
        + output_tokens / 1_000_000 * COST_PER_1M_OUTPUT
    )
    pipe = redis.pipeline()
    pipe.hincrbyfloat(f"usage:{user_id}:tokens", "input", input_tokens)
    pipe.hincrbyfloat(f"usage:{user_id}:tokens", "output", output_tokens)
    pipe.hincrbyfloat(f"usage:{user_id}:cost", "total", cost)
    pipe.hincrbyfloat("usage:global:cost", "total", cost)
    await pipe.execute()
    await redis.publish(
        "events:usage",
        json.dumps(
            {
                "user_id": user_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": round(cost, 6),
                "ts": datetime.now().isoformat(),
            }
        ),
    )


def parse_usage_from_stream(chunks: list[bytes]) -> tuple[int, int]:
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
                input_tokens = usage.get("prompt_tokens", 0)
                output_tokens = usage.get("completion_tokens", 0)
        except (json.JSONDecodeError, KeyError):
            pass
    return input_tokens, output_tokens
