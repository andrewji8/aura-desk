"""Liveness probe behaviour of the gateway HTTP surface."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi.testclient import TestClient


def test_health_ok(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "aura-gateway"
    assert body["environment"] == "local"
    assert body["timestamp"].endswith("Z") or "+00:00" in body["timestamp"]


def test_healthz_alias(client: TestClient) -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_versioned_health(client: TestClient) -> None:
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_request_id_is_echoed(client: TestClient) -> None:
    supplied = "abc123def456"

    response = client.get("/health", headers={"X-Request-ID": supplied})

    assert response.headers["X-Request-ID"] == supplied


def test_request_id_is_generated_when_absent(client: TestClient) -> None:
    response = client.get("/health")

    assert response.headers["X-Request-ID"]
