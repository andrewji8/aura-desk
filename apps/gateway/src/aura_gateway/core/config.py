"""Application settings, loaded from the environment and an optional ``.env`` file.

Every tunable value of the gateway is declared here. Nothing else in the code
base reads ``os.environ`` directly, so a single object is the only place to
audit when a deployment changes.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

__all__ = ["Environment", "Settings", "get_settings"]

Environment = Literal["local", "dev", "staging", "prod"]

#: PostgreSQL driver handled by SQLAlchemy's asyncio layer (``asyncpg``).
ASYNC_PG_SCHEME = "postgresql+asyncpg"


class Settings(BaseSettings):
    """Typed gateway configuration.

    Values are read from real environment variables first and from a ``.env``
    file as a fallback. Every field is prefixed with ``AURA_``, for example
    ``AURA_DATABASE_URL`` or ``AURA_LOG_LEVEL``. The instance is immutable so a
    request handler cannot mutate global state.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="AURA_",
        env_nested_delimiter="__",
        extra="ignore",
        frozen=True,
        case_sensitive=False,
    )

    # ---- application identity -------------------------------------------------
    PROJECT_NAME: str = "aura-gateway"
    VERSION: str = "0.1.0"
    ENVIRONMENT: Environment = "local"
    DEBUG: bool = False
    API_V1_PREFIX: str = "/api/v1"

    # ---- http server ----------------------------------------------------------
    HOST: str = "0.0.0.0"  # noqa: S104 - container entrypoint must bind all interfaces
    PORT: int = Field(default=8000, ge=1, le=65535)
    WORKERS: int = Field(default=1, ge=1, le=64)

    # ---- database -------------------------------------------------------------
    DATABASE_URL: str = f"{ASYNC_PG_SCHEME}://aura:aura@localhost:5432/aura_desk"
    DB_ECHO: bool = False
    DB_POOL_SIZE: int = Field(default=10, ge=1, le=100)
    DB_MAX_OVERFLOW: int = Field(default=10, ge=0, le=100)
    DB_POOL_TIMEOUT: float = Field(default=30.0, gt=0)
    DB_POOL_RECYCLE: int = Field(default=1800, ge=0)
    DB_CONNECT_ON_STARTUP: bool = True
    DB_FAIL_FAST_ON_STARTUP: bool = False

    # ---- observability --------------------------------------------------------
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True
    LOG_ACCESS: bool = True
    REQUEST_ID_HEADER: str = "X-Request-ID"

    # ---- LLM (OpenAI-compatible: DeepSeek / Zhipu / local vLLM) -------------
    # ``LLM_API_KEY`` is a SecretStr so it is masked in logs, tracebacks and
    # ``repr(Settings)``; call ``.get_secret_value()`` only at the request site.
    LLM_API_KEY: SecretStr = SecretStr("")
    LLM_BASE_URL: str = "https://api.deepseek.com/v1"
    LLM_MODEL: str = "deepseek-chat"
    LLM_SYSTEM_PROMPT: str = (
        "你是 Aura Desk 的虚拟客服助手. 请用简洁、自然的中文回答, 不要输出 Markdown 标题或表情符号."
    )
    LLM_TEMPERATURE: float = Field(default=0.7, ge=0.0, le=2.0)
    LLM_MAX_TOKENS: int = Field(default=512, ge=1, le=32_768)
    LLM_TIMEOUT_CONNECT: float = Field(default=5.0, gt=0, le=120)
    LLM_TIMEOUT_READ: float = Field(default=60.0, gt=0, le=600)
    # Bounded retry for rate limits and transient upstream errors. Retries only
    # apply before the first token is emitted, so a reply is never duplicated.
    LLM_MAX_RETRIES: int = Field(default=2, ge=0, le=5)
    LLM_RETRY_BACKOFF: float = Field(default=0.5, ge=0, le=30)
    # Shared connection pool for the process-wide AsyncClient.
    LLM_MAX_CONNECTIONS: int = Field(default=100, ge=1, le=1_000)

    # ---- CORS -----------------------------------------------------------------
    # ``NoDecode`` keeps pydantic-settings from JSON-parsing the raw environment
    # value; the validator below splits the comma-separated list instead.
    CORS_ORIGINS: Annotated[list[str], NoDecode] = Field(default_factory=list)
    CORS_ALLOW_CREDENTIALS: bool = False

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> str:
        """Upper-case the log level and reject anything the stdlib cannot map."""
        level = str(value).strip().upper()
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if level not in allowed:
            msg = f"AURA_LOG_LEVEL must be one of {sorted(allowed)}, got {value!r}"
            raise ValueError(msg)
        return level

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def _split_cors_origins(cls, value: object) -> list[str]:
        """Accept a JSON array or a comma-separated string of origins."""
        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(origin).strip() for origin in value if str(origin).strip()]
        msg = f"CORS origins must be a string or a sequence, got {type(value).__name__}"
        raise TypeError(msg)

    @field_validator("REQUEST_ID_HEADER", mode="after")
    @classmethod
    def _normalize_header_name(cls, value: str) -> str:
        """Validate the inbound header name used to propagate a request id."""
        if not value.strip():
            msg = "AURA_REQUEST_ID_HEADER must not be empty"
            raise ValueError(msg)
        return value.strip()

    @field_validator("LLM_BASE_URL", mode="after")
    @classmethod
    def _normalize_llm_base_url(cls, value: str) -> str:
        """Validate the upstream base URL and drop any trailing slash.

        Joining a pre-trimmed base URL with ``/chat/completions`` is what keeps
        a trailing slash from producing a double slash that some proxies reject.
        """
        normalized = value.strip().rstrip("/")
        if not normalized:
            msg = "AURA_LLM_BASE_URL must not be empty"
            raise ValueError(msg)
        scheme = normalized.split("://", maxsplit=1)[0].lower()
        if scheme not in {"http", "https"}:
            msg = f"AURA_LLM_BASE_URL must use http or https, got {value!r}"
            raise ValueError(msg)
        return normalized

    @field_validator("LLM_MODEL", mode="after")
    @classmethod
    def _normalize_llm_model(cls, value: str) -> str:
        """Reject a blank model name, which the upstream would reject anyway."""
        if not value.strip():
            msg = "AURA_LLM_MODEL must not be empty"
            raise ValueError(msg)
        return value.strip()

    @model_validator(mode="after")
    def _validate_runtime_combinations(self) -> Self:
        """Reject cross-field combinations that cannot produce a working app."""
        # Allow PostgreSQL in every environment, plus SQLite for local runs
        # where no database server is reachable.
        is_pg = self.DATABASE_URL.startswith(f"{ASYNC_PG_SCHEME}://")
        is_sqlite = self.DATABASE_URL.startswith("sqlite+aiosqlite://")

        if not (is_pg or is_sqlite):
            msg = (
                f"AURA_DATABASE_URL must start with '{ASYNC_PG_SCHEME}://' or 'sqlite+aiosqlite://'"
            )
            raise ValueError(msg)

        if self.DEBUG and self.ENVIRONMENT == "prod":
            msg = "DEBUG must be disabled in the prod environment"
            raise ValueError(msg)

        if "*" in self.CORS_ORIGINS and self.CORS_ALLOW_CREDENTIALS:
            msg = "CORS_ALLOW_CREDENTIALS must be false when CORS_ORIGINS contains '*'"
            raise ValueError(msg)

        # Plain HTTP is tolerated for a loopback vLLM, but must never reach a
        # remote host, where the API key would cross the network in clear text.
        if (
            self.is_production
            and self.LLM_BASE_URL.startswith("http://")
            and not self.LLM_BASE_URL.startswith(("http://127.0.0.1", "http://localhost"))
        ):
            msg = "AURA_LLM_BASE_URL must use https outside localhost in prod"
            raise ValueError(msg)

        return self

    @property
    def llm_configured(self) -> bool:
        """Whether an API key is present.

        A local vLLM deployment may legitimately run without one, so this is
        informational: the pipeline refuses to call the upstream only when the
        key is missing *and* the endpoint is remote.
        """
        return bool(self.LLM_API_KEY.get_secret_value())

    @property
    def llm_chat_completions_url(self) -> str:
        """Absolute URL of the OpenAI-compatible chat completions endpoint."""
        return f"{self.LLM_BASE_URL}/chat/completions"

    @property
    def is_production(self) -> bool:
        """Whether the process runs with production hardening enabled."""
        return self.ENVIRONMENT == "prod"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    The result is cached because building a ``Settings`` instance reads and
    parses the environment; every caller must observe the same values.
    """
    return Settings()
