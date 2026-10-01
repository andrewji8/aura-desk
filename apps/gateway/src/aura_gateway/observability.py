from aura_schemas.health import HealthResponse, HealthStatus

SERVICE_NAME = "aura-gateway"


def get_logger(name: str | None = None) -> object:
    import structlog

    return structlog.get_logger(name)


def health_payload() -> HealthResponse:
    return HealthResponse(health=HealthStatus(service=SERVICE_NAME))
