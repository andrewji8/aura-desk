"""Process-wide ``httpx.AsyncClient`` for upstream LLM calls.

One client per process is the only way to get connection pooling and keep-alive
reuse across sessions: a client built per request would pay a fresh TCP (and TLS)
handshake every turn. ``httpx.AsyncClient`` is safe to use from many concurrent
tasks, so all sessions share this instance.

Every method the pipeline uses (``stream``, ``aclose``) is a coroutine; no
synchronous HTTP client or SDK appears anywhere in the call path.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import httpx

from aura_gateway.core.config import Settings, get_settings
from aura_gateway.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = [
    "aclose_llm_client",
    "build_llm_client",
    "get_llm_client",
    "lifespan_llm",
    "set_llm_client",
]

_log = get_logger("aura_gateway.llm")

_client: httpx.AsyncClient | None = None


def build_llm_client(
    settings: Settings | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build the streaming client.

    Args:
        settings: Configuration source; defaults to the process singleton.
        transport: Optional transport override. Tests inject
            ``httpx.MockTransport`` so no socket is ever opened.

    Returns:
        A client whose timeout is split into connect and read phases, because a
        streaming response legitimately outlives a short connect budget.
    """
    active = settings or get_settings()
    keepalive = min(20, active.LLM_MAX_CONNECTIONS)
    return httpx.AsyncClient(
        timeout=httpx.Timeout(
            timeout=active.LLM_TIMEOUT_READ,
            connect=active.LLM_TIMEOUT_CONNECT,
        ),
        limits=httpx.Limits(
            max_connections=active.LLM_MAX_CONNECTIONS,
            max_keepalive_connections=keepalive,
        ),
        headers={"User-Agent": f"{active.PROJECT_NAME}/{active.VERSION}"},
        follow_redirects=False,
        transport=transport,
    )


def get_llm_client() -> httpx.AsyncClient:
    """Return the process-wide client, building it on first use.

    Building on demand rather than requiring the lifespan keeps unit tests and
    one-shot scripts working without an application context.

    Returns:
        The shared client.
    """
    global _client
    if _client is None or _client.is_closed:
        _client = build_llm_client()
        _log.info("llm.client_created", base_url=get_settings().LLM_BASE_URL)
    return _client


def set_llm_client(client: httpx.AsyncClient | None) -> None:
    """Install a client explicitly, used by the lifespan and by tests."""
    global _client
    _client = client


async def aclose_llm_client() -> None:
    """Close the shared client if one was created, releasing pooled sockets."""
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        _log.info("llm.client_closed")
    _client = None


@asynccontextmanager
async def lifespan_llm(settings: Settings | None = None) -> AsyncIterator[None]:
    """Own the shared client for the lifetime of the application.

    The client is created on entry and closed on exit, so keep-alive
    connections are not leaked when the process exits.
    """
    set_llm_client(build_llm_client(settings))
    try:
        yield
    finally:
        await aclose_llm_client()
