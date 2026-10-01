# aura-desk

AI virtual-human customer-service monorepo.

## Layout

```
apps/
  gateway/      # edge service: sessions, streaming transport, auth
packages/
  common/       # config, logging, db session, error taxonomy
  schemas/      # shared Pydantic contracts
```

## Requirements

- Python 3.11+
- Poetry 1.8+ (workspace support requires Poetry 2.x for the native
  `[tool.poetry.workspace]` table; with Poetry 1.8 use the
  `poetry-workspaces` plugin)

## Local setup

```bash
poetry install
poetry run pytest
poetry run ruff check .
poetry run ruff format .
```

## Container

```bash
docker build -f apps/gateway/Dockerfile -t aura-gateway:local .
```
