"""Structured JSON logging with per-request correlation.

``configure_logging`` wires :mod:`structlog` into the standard library logging
tree so that application logs and third-party logs (uvicorn, SQLAlchemy) are
emitted in the same JSON shape and share the same correlation keys.

Correlation ids are kept in a :class:`~contextvars.ContextVar` and mirrored into
structlog's own context variables, which lets any function deep in the call
stack emit ``log.info("...", ...fields...)`` and automatically receive the
``request_id`` of the request it is serving.
"""

from __future__ import annotations

from contextvars import ContextVar
import logging
import logging.config
import sys
from typing import TYPE_CHECKING, Any
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
import structlog

from aura_gateway.core.config import Settings, get_settings

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.requests import Request
    from starlette.responses import Response
    from starlette.types import ASGIApp

__all__ = [
    "RequestIdMiddleware",
    "bind_request_context",
    "clear_request_context",
    "configure_logging",
    "get_logger",
    "get_request_id",
    "new_request_id",
]

#: Correlation id of the request currently being served on this task.
_request_id_ctx: ContextVar[str | None] = ContextVar("aura_request_id", default=None)

#: Longest inbound header value accepted as a correlation id. Unbounded header
#: values would let a client bloat every log line it triggers.
_MAX_INBOUND_REQUEST_ID = 128


def new_request_id() -> str:
    """Generate a fresh correlation id for a request without an inbound one."""
    return uuid.uuid4().hex


def get_request_id() -> str | None:
    """Return the correlation id bound to the current context, if any."""
    return _request_id_ctx.get()


def bind_request_context(request_id: str, **extra: Any) -> None:
    """Bind ``request_id`` and extra fields for the current async context.

    Both this module's context variable and structlog's context store are
    updated so that ``get_request_id()`` works for non-logging callers.
    """
    _request_id_ctx.set(request_id)
    structlog.contextvars.bind_contextvars(request_id=request_id, **extra)


def clear_request_context() -> None:
    """Drop every bound correlation value, preventing leakage across requests."""
    _request_id_ctx.set(None)
    structlog.contextvars.clear_contextvars()


def _resolve_request_id(candidate: str | None) -> str:
    """Return a trustworthy correlation id.

    A client-supplied value is only honoured when it is short and printable;
    otherwise a server-generated id replaces it, because the value is written
    verbatim into every log line of the request.
    """
    if candidate:
        sanitized = candidate.strip()
        if 0 < len(sanitized) <= _MAX_INBOUND_REQUEST_ID and sanitized.isprintable():
            return sanitized
    return new_request_id()


def _json_or_console_renderer(log_json: bool) -> Any:
    """Return the renderer matching the configured output format."""
    if log_json:
        return structlog.processors.JSONRenderer(ensure_ascii=False)
    return structlog.dev.ConsoleRenderer(colors=False)


def configure_logging(settings: Settings | None = None) -> None:
    """Configure structlog and the stdlib logging tree.

    Safe to call repeatedly: handlers are replaced instead of accumulated.
    """
    active = settings or get_settings()
    level = getattr(logging, active.LOG_LEVEL, logging.INFO)
    renderer = _json_or_console_renderer(active.LOG_JSON)

    # Foreign (stdlib) records are rendered through the same chain so uvicorn
    # access logs and SQLAlchemy logs stay parseable alongside our own.
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]
    foreign_pre_chain: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "structured": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": renderer,
                    "foreign_pre_chain": foreign_pre_chain,
                },
            },
            "handlers": {
                # "()" is used instead of "class" so the handler object is used
                # directly; dictConfig can only import a "class" given by name.
                "default": {
                    "()": logging.StreamHandler,
                    "formatter": "structured",
                    "stream": sys.stdout,
                },
            },
            "loggers": {
                "": {"handlers": ["default"], "level": level, "propagate": False},
                "uvicorn": {"handlers": ["default"], "level": level, "propagate": False},
                "uvicorn.error": {"handlers": ["default"], "level": level, "propagate": False},
                "uvicorn.access": {
                    "handlers": ["default"],
                    "level": level if active.LOG_ACCESS else logging.WARNING,
                    "propagate": False,
                },
                "sqlalchemy.engine": {
                    "handlers": ["default"],
                    "level": logging.INFO if active.DB_ECHO else logging.WARNING,
                    "propagate": False,
                },
            },
        },
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger, optionally named after its module.

    The returned logger is a bound-logger proxy: it is cheap to create and safe
    to capture at import time, and it picks up context variables at call time.
    """
    return structlog.get_logger(name)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Bind a correlation id to every request and to every log line it emits.

    The middleware resolves the id once, binds it for the duration of the
    request, and echoes it back on the response so a caller can correlate a
    client-side failure with the server-side trace.
    """

    def __init__(self, app: ASGIApp, settings: Settings | None = None) -> None:
        super().__init__(app)
        self._settings = settings or get_settings()
        self._header = self._settings.REQUEST_ID_HEADER
        self._log = get_logger("aura_gateway.request")

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Attach a request id, time the call and emit one access log line."""
        request_id = _resolve_request_id(request.headers.get(self._header))
        bind_request_context(
            request_id,
            http_method=request.method,
            http_path=request.url.path,
            client_host=request.client.host if request.client else None,
        )
        self._log.info("request.started")

        try:
            response = await call_next(request)
        except Exception:
            self._log.exception("request.failed")
            clear_request_context()
            raise

        response.headers[self._header] = request_id
        request.state.request_id = request_id
        self._log.info("request.completed", status_code=response.status_code)
        clear_request_context()
        return response
