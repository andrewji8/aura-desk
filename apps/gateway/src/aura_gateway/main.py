from fastapi import FastAPI

from aura_gateway.observability import health_payload
from aura_schemas.health import HealthResponse

app = FastAPI(title="aura-gateway", version="0.1.0")


@app.get("/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return health_payload()
