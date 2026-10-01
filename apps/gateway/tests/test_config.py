"""Settings parsing, validation and logging configuration."""

from __future__ import annotations

from pydantic import ValidationError
import pytest

from aura_gateway.core.config import Settings
from aura_gateway.core.logging import _resolve_request_id, new_request_id


def test_defaults_are_sane(monkeypatch: pytest.MonkeyPatch) -> None:
    # conftest lowers the level globally to keep test output quiet; assert the
    # shipped default instead of the ambient one.
    monkeypatch.delenv("AURA_LOG_LEVEL", raising=False)
    monkeypatch.delenv("AURA_DATABASE_URL", raising=False)
    monkeypatch.delenv("AURA_ENVIRONMENT", raising=False)

    settings = Settings()

    assert settings.PROJECT_NAME == "aura-gateway"
    assert settings.DEBUG is False
    assert settings.DATABASE_URL.startswith("postgresql+asyncpg://")
    assert settings.LOG_LEVEL == "INFO"


def test_cors_origins_accept_comma_separated_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURA_CORS_ORIGINS", "https://a.example, https://b.example")

    assert Settings().CORS_ORIGINS == ["https://a.example", "https://b.example"]


def test_log_level_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURA_LOG_LEVEL", "debug")

    assert Settings().LOG_LEVEL == "DEBUG"


def test_invalid_log_level_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURA_LOG_LEVEL", "chatty")

    with pytest.raises(ValidationError):
        Settings()


def test_debug_in_prod_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(DEBUG=True, ENVIRONMENT="prod")


def test_wildcard_cors_with_credentials_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(CORS_ORIGINS=["*"], CORS_ALLOW_CREDENTIALS=True)


def test_non_async_database_url_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(DATABASE_URL="postgresql://aura@localhost/aura")


def test_resolve_request_id_accepts_sane_value() -> None:
    assert _resolve_request_id("  trace-1  ") == "trace-1"


def test_resolve_request_id_rejects_oversized_value() -> None:
    assert _resolve_request_id("x" * 500) != "x" * 500


def test_resolve_request_id_generates_when_missing() -> None:
    generated = _resolve_request_id(None)

    assert len(generated) == len(new_request_id())
