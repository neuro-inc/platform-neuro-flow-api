from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import TypeVar

import aiohttp.web


@asynccontextmanager
async def ndjson_error_handler(
    request: aiohttp.web.Request,
    response: aiohttp.web.StreamResponse,
) -> AsyncIterator[None]:
    try:
        yield
    except asyncio.CancelledError:
        raise
    except Exception as e:
        msg_str = (
            f"Unexpected exception {e.__class__.__name__}: {str(e)}. "
            f"Path with query: {request.path_qs}."
        )
        logging.exception(msg_str)
        payload = {"error": msg_str}
        await response.write(json.dumps(payload).encode())


T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


@asynccontextmanager
async def auto_close(
    gen: AsyncGenerator[T_co, T_contra],
) -> AsyncIterator[AsyncGenerator[T_co, T_contra]]:
    try:
        yield gen
    finally:
        with suppress(StopIteration):
            await gen.aclose()
