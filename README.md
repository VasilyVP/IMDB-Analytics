# IMDB Analytics

Full-stack app for graph analytics and instant querying over the IMDB dataset — Neo4j for graph traversals, DuckDB for relational queries, served via FastAPI to a React front-end.

## Architecture

```
IMDB TSV.GZ → Parquet (back-end/data/) → DuckDB (imdb.duckdb) → Neo4j graph
                                                  ↓
                                    FastAPI (back-end/) → React + Vite (front-end/)
```

| Layer | Technology |
|---|---|
| Relational store | DuckDB · Parquet |
| Graph store | Neo4j (Docker) |
| API | FastAPI · Granian |
| Front-end | React 19 · React Router 7 · TypeScript · Vite · Tailwind CSS 4 · shadcn/ui |

## Quick Start

```bash
# 1. Copy .env and set NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
# 2. Start Neo4j
make install

# 3. Seed data
make seed-sample   # ~1,000 titles (dev/testing)
# make seed        # full dataset (~30 min, ~1.5 GB)

# 4. Start dev servers
make dev
```

App: `http://localhost:3000` · API: `http://localhost:8000`
