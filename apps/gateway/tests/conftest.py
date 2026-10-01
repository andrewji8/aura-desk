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


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Yield a ``TestClient`` with the application lifespan running."""
    from aura_gateway.main import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """Drop the settings singleton so each test observes its own overrides."""
    from aura_gateway.core.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
