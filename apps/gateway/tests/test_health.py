from fastapi.testclient import TestClient

from aura_gateway.main import app


def test_healthz_ok() -> None:
    with TestClient(app) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"health": {"status": "ok", "service": "aura-gateway"}}
