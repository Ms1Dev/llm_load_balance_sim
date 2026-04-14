import asyncio
import os
import random

import httpx

MAX_RETRIES: int = 3
BASE_DELAY: float = 2.0
MAX_DELAY: float = 30.0
EXPONENTIAL_BASE: float = 2.0
JITTER: bool = True
# Upstream Retry-After can be huge; cap so the proxy does not sleep a full minute per 429.
RETRY_AFTER_CAP: float = float(os.environ.get("RETRY_AFTER_CAP", "15"))


def _parse_retry_after(headers: httpx.Headers) -> float | None:
    raw = headers.get("retry-after")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _calculate_delay(attempt: int) -> float:
    delay = BASE_DELAY * (EXPONENTIAL_BASE**attempt)
    delay = min(delay, MAX_DELAY)
    if JITTER:
        delay = delay * (0.5 + random.random())  # 0.5× – 1.5× multiplier
    return delay


async def sleep_backoff(attempt: int, retry_after_s: float | None = None) -> None:
    delay = _calculate_delay(attempt)
    if retry_after_s is not None:
        retry_after_s = min(retry_after_s, RETRY_AFTER_CAP)
        delay = max(delay, retry_after_s)
    await asyncio.sleep(delay)
