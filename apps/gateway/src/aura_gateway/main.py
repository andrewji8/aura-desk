"""FastAPI application factory and ASGI entrypoint.

Importing this module yields a configured ``app``. Logging, the database pool
and the error envelope are all wired here so that no other module needs to know
how the process is bootstrapped.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from aura_gateway.core.config import Settings, get_settings
from aura_gateway.core.database import get_engine_status, lifespan_db
from aura_gateway.core.exceptions import register_exception_handlers
from aura_gateway.core.logging import RequestIdMiddleware, configure_logging, get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

__all__ = ["HealthPayload", "app", "create_app", "lifespan"]

_log = get_logger("aura_gateway.main")


class HealthPayload(BaseModel):
    """Body returned by the liveness endpoint."""

    status: str = Field(default="ok", pattern=r"^(ok|degraded)$")
    service: str
    environment: str
    version: str
    timestamp: datetime
    database: dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging, then own the database pool for the process lifetime.

    Logging is configured before anything can emit a record so that startup and
    shutdown events are already structured. ``app.state.settings`` is populated
    here as well, giving handlers a typed handle on the configuration.
    """
    settings: Settings = getattr(app.state, "settings", None) or get_settings()
    app.state.settings = settings

    configure_logging(settings)
    _log.info(
        "app.starting",
        service=settings.PROJECT_NAME,
        version=settings.VERSION,
        environment=settings.ENVIRONMENT,
        debug=settings.DEBUG,
    )

    async with lifespan_db(settings):
        try:
            yield
        finally:
            _log.info("app.stopping", service=settings.PROJECT_NAME)


def _register_routers(app: FastAPI, settings: Settings) -> None:
    """Attach the HTTP surface of the service."""
    router = APIRouter()

    @router.get("/health", response_model=HealthPayload, summary="Liveness probe")
    async def health() -> HealthPayload:
        """Report process health and the current server time.

        Deliberately dependency-free: the liveness probe must answer even when
        a downstream database is degraded, so pool state is reported as data
        rather than awaited as a gate.
        """
        return HealthPayload(
            status="ok",
            service=settings.PROJECT_NAME,
            environment=settings.ENVIRONMENT,
            version=settings.VERSION,
            timestamp=datetime.now(UTC),
            database=get_engine_status(),
        )

    @router.get("/healthz", response_model=HealthPayload, summary="Compatibility probe")
    async def healthz() -> HealthPayload:
        """Alias of :func:`health` kept for the container HEALTHCHECK."""
        return await health()

    app.include_router(router)

    if settings.API_V1_PREFIX:
        v1 = APIRouter(prefix=settings.API_V1_PREFIX)

        @v1.get("/health", response_model=HealthPayload, summary="Versioned probe")
        async def health_v1() -> HealthPayload:
            """Expose the same probe under the versioned prefix."""
            return await health()

        app.include_router(v1)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build a fully wired application instance."""
    active = settings or get_settings()
    application = FastAPI(
        title=active.PROJECT_NAME,
        version=active.VERSION,
        debug=active.DEBUG,
        lifespan=lifespan,
    )
    application.state.settings = active

    register_exception_handlers(application)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=active.CORS_ORIGINS,
        allow_credentials=active.CORS_ALLOW_CREDENTIALS,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=[active.REQUEST_ID_HEADER],
    )
    # Registered last: it runs first, so the correlation id exists before any
    # other middleware or handler can log.
    application.add_middleware(RequestIdMiddleware, settings=active)
    _register_routers(application, active)
    return application


app = create_app()
