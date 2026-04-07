# Movies Project Guidelines

## Architecture

Data pipeline that ingests IMDB datasets into two query layers:

```
IMDB TSV.GZ → Parquet (back-end/data/) → DuckDB (imdb.duckdb) → Neo4j graph
```

- **DuckDB** — relational queries over Parquet files; single-file `back-end/data/imdb.duckdb`
- **Neo4j** — graph traversals; runs in Docker (browser: `localhost:7474`, bolt: `localhost:7687`)
- **Front-end** — not yet implemented (`front-end/` is empty)
- **Scripts** — all ETL logic lives in `scripts/`; no web API or server

## Data Schema

### DuckDB / Parquet Tables

| Table | Key Columns |
|-------|-------------|
| `title_basics` | `tconst`, `titleType`, `primaryTitle`, `originalTitle`, `isAdult`, `startYear`, `endYear`, `runtimeMinutes`, `genres` |
| `title_principals` | `tconst`, `nconst`, `category`, `job`, `characters` |
| `title_ratings` | `tconst`, `averageRating`, `numVotes` |
| `name_unique` | `nconst`, `primaryName`, `birthYear`, `deathYear`, `primaryProfession`, `knownForTitles` |

`name_unique` is filtered from `name.basics` — only persons linked to at least one title in `title_principals`. IMDB uses `\N` as a null sentinel; it's converted to SQL `NULL` during ingestion.

### Neo4j Graph Schema

**Nodes**: `Person` (PK: `nconst`), `Title` (PK: `tconst`, includes rating columns)

**Relationships** (direction: `Person → Title`), derived from `title_principals.category`:
`ACTED_IN`, `DIRECTED`, `WROTE`, `PRODUCED`, `COMPOSED`, `EDITED`, `SHOT`, `DESIGNED`, `CAST`, `APPEARED_IN`

Relationship properties: `category`, `job`, `characters` (all nullable). Unrecognised categories are normalised to `UPPER_SNAKE_CASE`.

## Build and Test

```bash
make install       # Start Neo4j container (docker compose up -d)
make seed          # Full data pipeline: download IMDB → Parquet → DuckDB → Neo4j (~30 min)
make seed-sample   # Seed pipeline with 1,000 titles — use this for dev/testing
make start         # Resume stopped containers
make stop          # Pause containers (data preserved)
make teardown      # Remove containers and networks (volumes kept)
make reset         # Full wipe: containers, networks, volumes, all data
make status        # Check running containers
make logs          # Tail logs
```

Run scripts directly: `uv run python scripts/<script>.py`

- `scripts/imdb_seed.py` — downloads IMDB datasets, converts to Parquet, rebuilds DuckDB
- `scripts/neo4j_seed.py [--limit N]` — seeds Neo4j from DuckDB; `--limit N` for a subset
- `scripts/csv_export.py` — exports Parquet to CSV (requires seeded Parquet files)

There are no tests yet; a `tests/` directory does not exist.

## Environment

Requires a `.env` file at project root:
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

Neo4j container must be running (`make install`) before seeding the graph. The container uses the APOC plugin and is configured with up to 6 GB heap.

## Conventions

- **Python 3.14+**, managed with `uv`; never use pip or npm directly
- **Strict Pyright** — all code must pass `pyrightconfig.json` rules; add type hints to every function
- Helper functions are prefixed with `_`; constants use `UPPER_SNAKE_CASE` at module top
- Use `from __future__ import annotations` for forward references
- Progress output uses `print(..., flush=True)`; no logging module is used

## Common Pitfalls

- `make seed` is slow (~30 min, ~1.5 GB download); use `make seed-sample` during development
- DuckDB holds an exclusive lock during Parquet conversion — don't query `imdb.duckdb` concurrently
- Parquet staleness check is 30 days; delete files manually to force a re-download
- If Pyright reports missing type stubs for a third-party library, add `# type: ignore` at the import
- Neo4j is seeded in batches of 5,000 records per transaction; maintain this pattern for bulk writes
- Use the `_int()` / `_float()` helpers when reading IMDB data that may contain `\N` sentinel values
