from pydantic import BaseModel, ConfigDict, Field


class HealthStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str = Field(default="ok", pattern=r"^(ok|degraded)$")
    service: str = "aura-gateway"


class HealthResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    health: HealthStatus


class ErrorDetail(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    message: str


class ErrorResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    detail: ErrorDetail
