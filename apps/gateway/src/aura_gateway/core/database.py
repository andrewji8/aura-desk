"""Asynchronous SQLAlchemy 2.0 engine, session factory and FastAPI wiring.

The engine is created lazily so importing this module never opens a socket, and
its disposal is owned by :func:`lifespan_db` so the pool is drained exactly once
when the process shuts down.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from aura_gateway.core.config import Settings, get_settings
from aura_gateway.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.engine import Engine
    from sqlalchemy.pool import Pool

__all__ = ["dispose_engine", "get_db", "get_engine", "get_sessionmaker", "lifespan_db"]

_log = get_logger("aura_gateway.db")

# Process-wide singletons. They are created on first use so that tests and
# management commands can import the module without touching the network.
_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def _engine_kwargs(settings: Settings) -> dict[str, Any]:
    """Build the keyword arguments shared by every engine instance."""
    return {
        "echo": settings.DB_ECHO,
        "pool_pre_ping": True,
        "pool_size": settings.DB_POOL_SIZE,
        "max_overflow": settings.DB_MAX_OVERFLOW,
        "pool_timeout": settings.DB_POOL_TIMEOUT,
        "pool_recycle": settings.DB_POOL_RECYCLE,
        "future": True,
    }


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    """Return the process-wide async engine, creating it on first call."""
    global _engine
    if _engine is None:
        active = settings or get_settings()
        _engine = create_async_engine(active.DATABASE_URL, **_engine_kwargs(active))
        _log.info("db.engine_created", pool_size=active.DB_POOL_SIZE)
    return _engine


def get_sessionmaker(settings: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    """Return the process-wide session factory bound to the async engine."""
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(
            bind=get_engine(settings),
            class_=AsyncSession,
            autoflush=False,
            expire_on_commit=False,
        )
    return _sessionmaker


async def dispose_engine() -> None:
    """Dispose the engine and drop the cached session factory.

    Safe to call when no engine was ever created, which keeps shutdown handlers
    free of conditionals.
    """
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
        _log.info("db.engine_disposed")
    _engine = None
    _sessionmaker = None


@asynccontextmanager
async def lifespan_db(settings: Settings | None = None) -> AsyncIterator[None]:
    """Own the connection pool for the lifetime of the application.

    Yields immediately so startup continues once the engine exists, and
    verifies connectivity when ``DB_CONNECT_ON_STARTUP`` is enabled. The
    verification failure is fatal only when ``DB_FAIL_FAST_ON_STARTUP`` is set,
    which lets the pod start and surface the fault through ``/health`` instead
    of crash-looping.
    """
    active = settings or get_settings()
    engine = get_engine(active)
    get_sessionmaker(active)

    if active.DB_CONNECT_ON_STARTUP:
        try:
            async with engine.connect() as connection:
                await connection.exec_driver_sql("SELECT 1")
        except SQLAlchemyError:
            _log.exception("db.startup_probe_failed")
            if active.DB_FAIL_FAST_ON_STARTUP:
                await dispose_engine()
                raise
            # Otherwise startup continues: the pool is lazily (re)connecting and
            # /health reports the degradation instead of crash-looping the pod.
        else:
            _log.info("db.startup_probe_succeeded")

    try:
        yield
    finally:
        await dispose_engine()


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a transactional :class:`AsyncSession`.

    The session is rolled back on failure so a handler that raises cannot leave
    a connection in a failed transaction state for the next borrower.
    """
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def get_engine_status() -> dict[str, Any]:
    """Summarise pool state for health and diagnostics endpoints."""
    if _engine is None:
        return {"created": False, "pool": None}

    pool: Pool | None = _engine.pool
    raw: Engine = _engine.sync_engine
    return {
        "created": True,
        "dialect": raw.dialect.name,
        "pool": type(pool).__name__ if pool is not None else None,
    }
