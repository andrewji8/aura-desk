"""Global exception envelope and request-id propagation."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.testclient import TestClient

from aura_gateway.core.exceptions import AppException, NotFoundError, RateLimitError
from aura_gateway.main import create_app


def _app_with_failing_route() -> object:
    """Build an app exposing routes that raise known exception types."""
    application = create_app()
    router = APIRouter()

    @router.get("/boom")
    async def boom() -> None:
        raise NotFoundError("session not found")

    @router.get("/limited")
    async def limited() -> None:
        raise RateLimitError(details={"retry_after": 30})

    @router.get("/kaboom")
    async def kaboom() -> None:
        raise RuntimeError("secret internal detail")

    application.include_router(router)
    return application


def test_app_exception_maps_to_envelope() -> None:
    client = TestClient(_app_with_failing_route(), raise_server_exceptions=False)

    response = client.get("/boom")

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "not_found"
    assert body["message"] == "session not found"
    assert "request_id" in body


def test_app_exception_details_are_included() -> None:
    client = TestClient(_app_with_failing_route(), raise_server_exceptions=False)

    body = client.get("/limited").json()

    assert body["code"] == "rate_limited"
    assert body["details"] == {"retry_after": 30}


def test_unhandled_exception_hides_internals() -> None:
    client = TestClient(_app_with_failing_route(), raise_server_exceptions=False)

    response = client.get("/kaboom")

    assert response.status_code == 500
    body = response.json()
    assert body["code"] == "internal_error"
    assert body["message"] == "Internal server error"
    assert "secret internal detail" not in response.text
    assert "Traceback" not in response.text


def test_request_id_matches_error_payload() -> None:
    client = TestClient(_app_with_failing_route(), raise_server_exceptions=False)
    supplied = "trace-me-42"

    response = client.get("/boom", headers={"X-Request-ID": supplied})

    assert response.json()["request_id"] == supplied


def test_custom_code_overrides_default() -> None:
    class TeapotError(AppException):
        default_code = "teapot"
        status_code = 418

    assert TeapotError().code == "teapot"
    assert TeapotError(code="short_and_stout").code == "short_and_stout"


def test_unknown_route_uses_envelope() -> None:
    client = TestClient(_app_with_failing_route())

    body = client.get("/nope").json()

    assert body["code"] == "http_404"
    assert body["request_id"]
