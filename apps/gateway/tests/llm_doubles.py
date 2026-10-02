"""Test doubles for the upstream LLM.

``httpx.MockTransport`` intercepts at the transport layer, so the code under test
is the production path: ``client.stream``, ``aiter_lines``, SSE framing and
error mapping. No socket is opened and no production branch exists solely for
tests.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
import json
from typing import Any

import httpx

__all__ = [
    "AsyncByteStream",
    "delayed_sse_bytes",
    "error_response",
    "mock_client",
    "sse_bytes",
    "sse_response",
    "token_frames",
    "token_response",
]

#: Anything callable with the request and returning a response or stream.
Handler = Callable[[httpx.Request], Any]


class AsyncByteStream(httpx.AsyncByteStream):
    """Adapt a plain async iterator to the interface httpx expects.

    ``httpx.Response(stream=...)`` rejects anything that is not an
    ``AsyncByteStream``, so chunk-at-a-time bodies must be wrapped to be
    genuinely incremental rather than pre-buffered.
    """

    def __init__(self, chunks: AsyncIterator[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._chunks:
            yield chunk


def sse_bytes(*lines: str) -> AsyncIterator[bytes]:
    """Encode raw SSE lines into a chunked byte stream.

    Args:
        *lines: Lines as they appear on the wire, without the trailing blank
            line; each is emitted separately so multi-line events and
            keep-alive comments can be expressed.

    Returns:
        An async byte iterator usable as an ``httpx`` stream body.
    """

    async def stream() -> AsyncIterator[bytes]:
        for line in lines:
            yield f"{line}\n".encode()
        yield b"\n"

    return stream()


def delayed_sse_bytes(*lines: str, interval: float = 0.01) -> AsyncIterator[bytes]:
    """Like :func:`sse_bytes` but with a pause before each line.

    Args:
        *lines: Lines as they appear on the wire.
        interval: Seconds to wait before emitting each line.

    Returns:
        An async byte iterator that yields slowly enough for a barge-in to land
        while the stream is still open.
    """

    async def stream() -> AsyncIterator[bytes]:
        for line in lines:
            await asyncio.sleep(interval)
            yield f"{line}\n".encode()
        yield b"\n"

    return stream()


def token_frames(*tokens: str) -> list[str]:
    """Render one OpenAI-compatible SSE chunk per token."""
    return [
        f"data: {json.dumps({'choices': [{'index': 0, 'delta': {'content': token}}]}, ensure_ascii=False)}"
        for token in tokens
    ]


def sse_response(*lines: str, status_code: int = 200) -> httpx.Response:
    """Build a streaming response whose body is exactly ``lines``."""
    return httpx.Response(
        status_code,
        headers={"content-type": "text/event-stream"},
        stream=AsyncByteStream(sse_bytes(*lines)),
    )


def token_response(*tokens: str) -> httpx.Response:
    """Build a complete, well-formed streaming reply for ``tokens``."""
    return sse_response(*token_frames(*tokens), "data: [DONE]")


def error_response(status_code: int, message: str = "boom") -> httpx.Response:
    """Build a JSON error body shaped like an OpenAI-compatible error."""
    return httpx.Response(
        status_code,
        json={"error": {"message": message, "type": "invalid_request_error"}},
    )


def mock_client(handler: Handler) -> httpx.AsyncClient:
    """Return a client whose every request is answered by ``handler``."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))
