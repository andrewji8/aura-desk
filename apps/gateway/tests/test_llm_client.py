"""Lifecycle of the process-wide LLM HTTP client."""

from __future__ import annotations

import httpx
from llm_doubles import token_response
import pytest

from aura_gateway.core.llm import (
    aclose_llm_client,
    build_llm_client,
    get_llm_client,
    lifespan_llm,
    set_llm_client,
)


@pytest.fixture(autouse=True)
def _clear_client() -> None:
    """Ensure no test leaks a client into the next one."""
    set_llm_client(None)
    yield
    set_llm_client(None)


def test_get_llm_client_builds_on_demand() -> None:
    client = get_llm_client()

    assert isinstance(client, httpx.AsyncClient)
    assert get_llm_client() is client  # the same instance, so pooling works


async def test_lifespan_installs_and_closes_the_client() -> None:
    async with lifespan_llm():
        installed = get_llm_client()
        assert not installed.is_closed

    assert installed.is_closed


async def test_aclose_is_safe_without_a_client() -> None:
    await aclose_llm_client()  # must not raise


def test_build_llm_client_honours_settings() -> None:
    client = build_llm_client(transport=httpx.MockTransport(lambda request: token_response("x")))

    assert client.headers["user-agent"].startswith("aura-gateway/")
    assert isinstance(client.timeout, httpx.Timeout)
    assert isinstance(client.timeout.connect, float)


async def test_mock_transport_is_used_when_injected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return token_response("injected")

    client = build_llm_client(transport=httpx.MockTransport(handler))
    try:
        response = await client.get("https://example.invalid/probe")
        assert response.status_code == 200
        assert b"injected" in response.content
    finally:
        await client.aclose()
