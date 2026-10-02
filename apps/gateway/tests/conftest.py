"""Shared pytest fixtures and environment overrides for the gateway tests.

The environment must be set before ``aura_gateway.core.config`` is imported,
because ``Settings`` is cached at first use.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

    import httpx

# No database is reachable from the test runner; the engine still gets built so
# the pool wiring is exercised, but nothing attempts a real handshake.
#
# These are assigned, not `setdefault`: a developer's local `.env` is read by
# `Settings` and would otherwise leak DEBUG=true and a sqlite URL into the suite.
os.environ["AURA_DEBUG"] = "false"
os.environ["AURA_DB_CONNECT_ON_STARTUP"] = "false"
os.environ["AURA_DB_FAIL_FAST_ON_STARTUP"] = "false"
os.environ["AURA_DATABASE_URL"] = "postgresql+asyncpg://aura:aura@localhost:5432/aura_test"
os.environ["AURA_LOG_LEVEL"] = "WARNING"
os.environ["AURA_ENVIRONMENT"] = "local"
os.environ["AURA_LLM_RETRY_BACKOFF"] = "0"

#: Deterministic reply served to websocket tests by the mocked upstream.
MOCK_REPLY_TOKENS = ("您好", ", ", "我是", "Aura", "虚拟助手", ". ", "请问有什么可以帮您?")


@pytest.fixture
def mock_llm() -> Iterator[httpx.AsyncClient]:
    """Point the process-wide LLM client at a mocked upstream.

    The websocket endpoint builds its pipeline from the shared client, so
    swapping that client is what keeps the integration tests off the network
    without adding a test-only branch to production code.
    """
    from llm_doubles import mock_client, token_response

    from aura_gateway.core.llm import set_llm_client

    client = mock_client(lambda request: token_response(*MOCK_REPLY_TOKENS))
    set_llm_client(client)
    try:
        yield client
    finally:
        # MockTransport owns no sockets, so the client is simply dereferenced
        # rather than closed from a synchronous teardown.
        set_llm_client(None)


@pytest.fixture
def client(mock_llm: httpx.AsyncClient) -> Iterator[TestClient]:
    """Yield a ``TestClient`` with the application lifespan running."""
    from aura_gateway.core.llm import set_llm_client
    from aura_gateway.main import app

    with TestClient(app) as test_client:
        # Entering the lifespan installs a real client; restore the mock so any
        # session opened through this app still never touches the network.
        set_llm_client(mock_llm)
        yield test_client


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """Drop the settings singleton so each test observes its own overrides."""
    from aura_gateway.core.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
