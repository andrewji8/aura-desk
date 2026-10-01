# syntax=docker/dockerfile:1.10
#
# Build from the REPO ROOT (workspace-wide paths are used below):
#   docker build -f apps/gateway/Dockerfile -t aura-gateway:local .
#
# Strategy: `deps` resolves every third-party dependency from poetry.lock into a
# real virtualenv; `builder` produces one wheel per workspace member; `runner`
# copies that virtualenv and installs the member wheels with --no-deps so the
# locked versions win and nothing is re-resolved from PyPI at runtime.

ARG PYTHON_VERSION=3.11
ARG POETRY_VERSION=2.1.3
ARG APP_UID=10001
ARG APP_GID=10001

# ---------- stage 1: resolve locked dependencies into /venv ----------
FROM python:${PYTHON_VERSION}-slim AS deps
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true
WORKDIR /src
COPY pyproject.toml poetry.lock ./
COPY apps/gateway/pyproject.toml ./apps/gateway/
COPY packages/common/pyproject.toml ./packages/common/
COPY packages/schemas/pyproject.toml ./packages/schemas/
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    pip install "poetry==${POETRY_VERSION}" \
 && poetry install --only main --no-root \
 && /src/.venv/bin/python -c "import fastapi, uvicorn, structlog, asyncpg, sqlalchemy"

# ---------- stage 2: build a wheel for every workspace member ----------
FROM deps AS builder
COPY packages/common ./packages/common
COPY packages/schemas ./packages/schemas
COPY apps/gateway ./apps/gateway
RUN --mount=type=cache,target=/root/.cache/pypoetry \
    set -eux; \
    mkdir -p /dist; \
    for member in packages/common packages/schemas apps/gateway; do \
        cd "/src/$member"; \
        poetry build -f wheel; \
        cp dist/*.whl /dist/; \
    done; \
    ls -1 /dist

# ---------- stage 3: minimal runtime ----------
FROM python:${PYTHON_VERSION}-slim AS runner
ARG APP_UID
ARG APP_GID
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV=/app/.venv

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends libpq5 curl; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --gid "$APP_GID" aura; \
    useradd --uid "$APP_UID" --gid "$APP_GID" --create-home --shell /usr/sbin/nologin aura

WORKDIR /app
COPY --from=deps /src/.venv /app/.venv
COPY --from=builder /dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir --no-index --no-deps /tmp/wheels/*.whl \
 && rm -rf /tmp/wheels \
 && python -c "import aura_gateway, aura_common, aura_schemas, fastapi, uvicorn"

USER aura:aura
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1

ENTRYPOINT ["python", "-m", "aura_gateway"]
