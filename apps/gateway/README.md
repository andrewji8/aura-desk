# aura-gateway

Edge service for aura-desk: terminates client sessions, proxies avatar stream
frames to the inference tier, and enforces auth plus rate limits.

Run locally:

```bash
poetry install
poetry run uvicorn aura_gateway.main:app --reload
```
