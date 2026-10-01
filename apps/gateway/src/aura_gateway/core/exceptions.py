"""Typed application exceptions and their global FastAPI handlers.

Every error that reaches a client is rendered by one of the handlers in this
module, so the wire format is identical no matter where the failure started:
``{"code": ..., "message": ..., "request_id": ...}``. Internal details such as
tracebacks and SQL statements never cross the process boundary.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from aura_gateway.core.logging import get_logger, get_request_id

if TYPE_CHECKING:
    from fastapi import FastAPI, Request

__all__ = [
    "AppException",
    "ConfigurationError",
    "ConflictError",
    "ExternalServiceError",
    "NotFoundError",
    "PermissionDeniedError",
    "RateLimitError",
    "UnauthorizedError",
    "ValidationError",
    "build_error_response",
    "register_exception_handlers",
]

_log = get_logger("aura_gateway.exceptions")


class AppException(Exception):  # noqa: N818 - public API name mandated by the platform spec
    """Base class for expected, client-visible failures.

    Subclasses set :attr:`default_code` and :attr:`status_code`; an instance
    may override them to describe a more specific failure. Raising this type
    never logs a traceback, because the failure is part of normal control flow.
    """

    default_code: str = "app_error"
    status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR

    def __init__(
        self,
        message: str | None = None,
        *,
        code: str | None = None,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        resolved_message = message or "Unexpected application error"
        super().__init__(resolved_message)
        self.message = resolved_message
        self.code = code or self.default_code
        self.status_code = status_code or self.status_code
        self.details = details or {}


class ValidationError(AppException):
    """The request was syntactically valid but semantically rejected."""

    default_code = "validation_error"
    status_code = HTTPStatus.UNPROCESSABLE_ENTITY


class NotFoundError(AppException):
    """A referenced resource does not exist."""

    default_code = "not_found"
    status_code = HTTPStatus.NOT_FOUND


class ConflictError(AppException):
    """The request collides with the current state of the resource."""

    default_code = "conflict"
    status_code = HTTPStatus.CONFLICT


class UnauthorizedError(AppException):
    """Authentication credentials are missing or invalid."""

    default_code = "unauthorized"
    status_code = HTTPStatus.UNAUTHORIZED


class PermissionDeniedError(AppException):
    """The caller is authenticated but not allowed to perform the action."""

    default_code = "permission_denied"
    status_code = HTTPStatus.FORBIDDEN


class RateLimitError(AppException):
    """The caller exceeded the allowed request rate."""

    default_code = "rate_limited"
    status_code = HTTPStatus.TOO_MANY_REQUESTS


class ConfigurationError(AppException):
    """The service is misconfigured and cannot serve traffic safely."""

    default_code = "configuration_error"
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR


class ExternalServiceError(AppException):
    """A downstream dependency failed or returned an unusable response."""

    default_code = "external_service_error"
    status_code = HTTPStatus.BAD_GATEWAY


def build_error_response(
    *,
    code: str,
    message: str,
    status_code: int,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    """Render the canonical error envelope, including the correlation id."""
    payload: dict[str, Any] = {
        "code": code,
        "message": message,
        "request_id": get_request_id(),
    }
    if details:
        payload["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


async def app_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Convert an :class:`AppException` into the standard error envelope."""
    if not isinstance(exc, AppException):
        msg = "app_exception_handler received a non-AppException"
        raise TypeError(msg) from exc
    _log.info(
        "app.error",
        code=exc.code,
        status_code=exc.status_code,
        path=request.url.path,
    )
    return build_error_response(
        code=exc.code,
        message=exc.message,
        status_code=exc.status_code,
        details=exc.details or None,
    )


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Normalise Starlette/FastAPI ``HTTPException`` into the same envelope."""
    if not isinstance(exc, StarletteHTTPException):
        msg = "http_exception_handler received a non-HTTPException"
        raise TypeError(msg) from exc
    # 5xx means an unhandled condition; its detail may contain internals.
    if exc.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
        _log.error(
            "app.unhandled_http_error",
            status_code=exc.status_code,
            path=request.url.path,
        )
        message = HTTPStatus(exc.status_code).phrase
    else:
        message = str(exc.detail)
    return build_error_response(
        code=f"http_{exc.status_code}",
        message=message,
        status_code=exc.status_code,
    )


async def validation_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Report request-body/query validation failures without echoing secrets."""
    if not isinstance(exc, RequestValidationError):
        msg = "validation_exception_handler received an unexpected error"
        raise TypeError(msg) from exc
    _log.info(
        "app.request_validation_failed",
        error_count=len(exc.errors()),
        path=request.url.path,
    )
    return build_error_response(
        code="request_validation_error",
        message="Request payload failed validation",
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        details={"errors": exc.errors()},
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Last-resort handler: log the traceback, return an opaque 500."""
    _log.exception("app.unhandled_exception", path=request.url.path, exc_info=exc)
    return build_error_response(
        code="internal_error",
        message="Internal server error",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Install the global exception handlers on ``app``.

    ``AppException`` is registered first because FastAPI resolves handlers by
    exact class, and the catch-all must be registered last so it only sees
    exceptions nothing else claimed.
    """
    app.add_exception_handler(AppException, app_exception_handler)  # pyright: ignore[reportArgumentType]
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # pyright: ignore[reportArgumentType]
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # pyright: ignore[reportArgumentType]
    app.add_exception_handler(Exception, unhandled_exception_handler)
