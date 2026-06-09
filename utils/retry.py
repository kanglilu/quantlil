from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar


T = TypeVar("T")


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    logger: logging.Logger | None = None,
    operation_name: str = "operation",
) -> T:
    last_exc: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break

            delay = base_delay * (2 ** (attempt - 1))
            if logger:
                logger.warning(
                    "%s failed on attempt %s/%s: %s. Retrying in %.1fs",
                    operation_name,
                    attempt,
                    attempts,
                    exc,
                    delay,
                )
            await asyncio.sleep(delay)

    assert last_exc is not None
    raise last_exc

