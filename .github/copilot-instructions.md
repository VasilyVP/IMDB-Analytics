# Movies Project Agent Guide

This file gives AI coding agents the fastest path to productive changes. Keep this guide minimal and prefer links to source docs.

## Quick Links

- Architecture and setup: [README.md](../README.md)
- Canonical commands: [Makefile](../Makefile)
- Back-end contracts: [specs/back-end/](../specs/back-end/)
- Front-end contracts: [specs/front-end/](../specs/front-end/)
- Data seeding details: [specs/data_seeding/](../specs/data_seeding/)
- Front-end HTTP pattern: [specs/patterns/front-end/custom_http_hook.md](../specs/patterns/front-end/custom_http_hook.md)

## Architecture Snapshot

```text
IMDB TSV.GZ -> Parquet -> DuckDB + Neo4j + ChromaDB
                      |
                  FastAPI API (back-end)
                      |
                React + Vite (front-end)
```

- DuckDB: relational analytics over parquet-backed tables
- Neo4j: graph traversal queries
- ChromaDB: similarity search and title-description indexing
- FastAPI: N-tier flow endpoints -> services -> repositories -> database

## Daily Workflow

```bash
make install       # start infra (Neo4j + ChromaDB)
make seed-sample   # dev dataset (~1,000 titles)
make dev           # FastAPI + Vite
make test          # backend + scripts unit tests
```

Optional LLM profile:

```bash
make install:llm
make start:llm
```

## Required Environment

Create `.env` at repo root with:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

## Backend Rules (Python)

- Use `uv` only (no pip)
- Keep strict typing compatible with [pyrightconfig.json](../pyrightconfig.json)
- Add type hints to every function
- Use `from __future__ import annotations` for new Python modules
- Keep helper names prefixed with `_` and constants in `UPPER_SNAKE_CASE`
- Maintain N-tier boundaries: endpoints should call services, services call repositories
- Inject DuckDB via `DuckDBDep` from [back-end/app/api/dependencies.py](../back-end/app/api/dependencies.py)
- Register startup work in `STARTUP_ACTIONS` in [back-end/app/core/startup.py](../back-end/app/core/startup.py)
- Repository SQL should stay as module-level `_*_TEMPLATE` constants with `{source_table}` placeholders
- Use `_resolve_source_relation()` when mapping toggle combinations to source relations
- For independent query fan-out in services, use `ThreadPoolExecutor(max_workers=4)`
- Use `print(..., flush=True)` for progress output (no logging framework)
- Keep Pydantic schemas under `schemas/`; `models/` is intentionally empty
- Unit tests must use fakes/mocks (no real DuckDB/Neo4j connections)

## Frontend Rules (TypeScript)

- Use Bun only (`bun add`, `bun run`), never npm
- Respect strict TypeScript configuration
- Keep query keys stable in React Query hooks
- Use `keepPreviousData` for refetch transitions when applicable
- Manage filters in Analytics route state; `FilterPanel` stays controlled
- Use `use-immer` update style for filter mutations
- Keep alias `@` for imports from `src/`
- Add shadcn components with `bunx --bun shadcn@latest add <component>`
- Prefer `@hugeicons/react` icons, then `lucide-react`

## Data and Seeding Rules

- Prefer `make seed-sample` for iteration; `make seed` is slow
- IMDB sentinel `\N` must become SQL `NULL`
- Use `_int()` and `_float()` helpers for nullable numeric parse logic
- Neo4j bulk writes must remain batched at 5,000 records per transaction
- DuckDB may hold exclusive locks during conversion; avoid concurrent reads then
- Parquet staleness is 30 days; delete source files to force refresh when needed

## Test and Validation Commands

```bash
make test
make test:integrational
uv run pyright
uv run pyright scripts
cd front-end && bun run tsc -b --noEmit
cd front-end && bun run build
```

## Change Hygiene for Agents

- Update the corresponding spec file when changing API behavior or feature contracts
- Keep edits minimal and avoid unrelated refactors
- Prefer linking docs over duplicating large explanation blocks in instruction files
