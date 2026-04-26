# IMDB Insights

Full-stack app for graph analytics and instant querying over the IMDB dataset — Neo4j for graph traversals, DuckDB for relational queries, and Chroma DB for similarity search and storing film descriptions, served via FastAPI to a React front-end. It also includes AI-assisted exploration features that let you ask natural-language questions and discover related titles through semantic search, with OpenAI-compatible LLM backends via Docker Compose vLLM profile, local Ollama, or remote model providers.

The project is designed for fast iteration:

- ETL pipeline from IMDB TSV files to queryable stores
- REST API layer with clear endpoint -> service -> repository boundaries
- Interactive analytics UI with filtering, search, and graph visualizations

## Architecture

```
IMDB TSV.GZ → Parquet (back-end/data/) → DuckDB (imdb.duckdb) → Neo4j graph
                                                \             ↘
                                                 \→ Chroma DB (similarity + film descriptions)
                                                  ↓
                            LLM (vLLM via Docker profile | Ollama | remote OpenAI-compatible)
                                                  ↓
                                    FastAPI (back-end/) → React + Vite (front-end/)
```

| Layer | Technology |
|---|---|
| Relational store | DuckDB · Parquet |
| Graph store | Neo4j (Docker) |
| Vector store | Chroma DB (similarity search + film descriptions) |
| LLM provider | vLLM (Docker profile) · Ollama · Remote OpenAI-compatible API |
| API | FastAPI · Granian |
| Front-end | React 19 · React Router 7 · TypeScript · Vite · Tailwind CSS 4 · shadcn/ui |

## Core Capabilities

- Analytics queries from columnar data stores over IMDB titles, people, and ratings
- Graph traversal use cases powered by Neo4j relationship modeling
- Similarity search over title descriptions with Chroma DB
- AI-assisted data exploration with natural-language query support and semantic discovery
- Filter-driven front-end workflows backed by stable API contracts

## Project Layout

| Path | Purpose |
|---|---|
| `back-end/app/` | FastAPI app (API endpoints, services, repositories, schemas, core) |
| `back-end/tests/` | Unit tests for API behavior and startup/config checks |
| `scripts/` | Data ingestion and seeding scripts (IMDB, Neo4j, Chroma) |
| `scripts/tests/` | Script-level unit and integration tests |
| `front-end/src/` | React app (routes, features, hooks, UI components) |
| `specs/` | Feature and architecture specifications |

## Quick Start

```bash
# 1. Copy .env and set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
# 2. Start infrastructure services (Neo4j, ChromaDB)
make install

# Optional: include vLLM when needed
# make install:llm

# 3. Seed data
make seed-sample   # ~1,000 titles (dev/testing)
# make seed        # full dataset (~30 min, ~1.5 GB)

# 4. Start dev servers
make dev
```

App: `http://localhost:3000` · API: `http://localhost:8000`

## Environment

Create a `.env` file in the repository root with:

- `NEO4J_URI` (example: `bolt://localhost:7687`)
- `NEO4J_USER` (example: `neo4j`)
- `NEO4J_PASSWORD` (example: `password`)

## Specifications

Detailed behavior and contracts are documented in `specs/`:

- Back-end endpoint contracts: `specs/back-end/`
- Front-end behavior and filtering flow: `specs/front-end/`
- Data seeding and graph model details: `specs/data_seeding/`

## Notes

- Use `make seed-sample` for development feedback loops; full seeding is intentionally heavier.
- DuckDB and Neo4j are complementary: relational aggregations in DuckDB, graph traversals in Neo4j.
- Chroma DB extends search beyond exact matching by enabling semantic similarity lookups.
