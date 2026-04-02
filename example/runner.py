import asyncio
import json
import os
import random
import time

import aiohttp
import redis as redis_lib
from django.utils import timezone

BASE_URL = os.environ.get("OPENAI_BASE_URL", "http://relay:8002/v1")
API_KEY = os.environ.get("OPENAI_API_KEY", "dummy")
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

TIME_SCALE = 4  # Match relay — compresses simulated time so a 30s run spans two window cycles


def _publish(run_pk: int, tool: str, args: dict):
    r = redis_lib.from_url(REDIS_URL)
    r.publish(f"run:{run_pk}", json.dumps({"tool": tool, "args": args}))

_LOREM = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
    "incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud "
    "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute "
    "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
    "pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia "
    "deserunt mollit anim id est laborum sed ut perspiciatis unde omnis iste natus error "
    "sit voluptatem accusantium doloremque laudantium totam rem aperiam eaque ipsa quae "
    "ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo "
    "nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit sed quia "
    "consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt neque porro "
    "quisquam est qui dolorem ipsum quia dolor sit amet consectetur adipisci velit"
).split()


def _random_prompt(target_words: int = 20) -> str:
    low = max(5, int(target_words * 0.5))
    high = min(len(_LOREM) - 1, int(target_words * 1.5))
    length = random.randint(low, high)
    start = random.randint(0, len(_LOREM) - length)
    words = _LOREM[start:start + length]
    words[0] = words[0].capitalize()
    return " ".join(words) + "?"


async def _save_result(run_user, started_at, latency_ms, input_tokens, output_tokens, success, error):
    """Save a single RunResult to the DB in a thread pool to avoid blocking the event loop."""
    from .models import RunResult
    await asyncio.to_thread(
        RunResult.objects.create,
        run=run_user.run,
        run_user=run_user,
        started_at=started_at,
        latency_ms=latency_ms,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        success=success,
        error=error,
    )
    await asyncio.to_thread(
        _publish,
        run_user.run_id,
        "run_result",
        {
            "user_pk": run_user.pk,
            "started_at": started_at.strftime("%H:%M:%S"),
            "latency_ms": latency_ms,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "success": success,
            "error": error,
        },
    )


async def _run_user_loop(session: aiohttp.ClientSession, run_user, end_time: float):
    """Drive a single user: request → save result → delay → repeat until end_time."""
    while time.monotonic() < end_time:
        prompt = _random_prompt(run_user.prompt_words)
        started_at = timezone.now()
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
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": True,
                    "stream_options": {"include_usage": True},
                    "temperature": 1,
                    "n": 1,
                },
                headers={"Authorization": f"Bearer {API_KEY}"},
            ) as resp:
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
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass

            if not error:
                success = True

        except Exception as exc:
            error = str(exc)

        latency_ms = (time.monotonic() - t0) * 1000

        await _save_result(run_user, started_at, latency_ms, input_tokens, output_tokens, success, error)

        remaining = end_time - time.monotonic()
        if remaining <= 0:
            break
        await asyncio.sleep(min(60.0 / run_user.rpm / TIME_SCALE, remaining))


async def _execute(users: list, duration_seconds: int):
    end_time = time.monotonic() + duration_seconds

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[
            _run_user_loop(session, user, end_time)
            for user in users
        ])


def execute_run(run):
    """
    Synchronous entry point called from a background thread — creates its own event loop.
    Results are written to the DB incrementally as each request completes.
    """
    from .models import Run

    users = list(run.users.all())
    duration_seconds = run.config_snapshot['duration_seconds']

    run.status = Run.Status.RUNNING
    run.save(update_fields=["status"])
    _publish(run.pk, "run_status", {"status": Run.Status.RUNNING})

    try:
        asyncio.run(_execute(users, duration_seconds))
        run.status = Run.Status.COMPLETED
    except Exception:
        run.status = Run.Status.FAILED
        raise
    finally:
        run.save(update_fields=["status"])
        _publish(run.pk, "run_status", {"status": run.status})
