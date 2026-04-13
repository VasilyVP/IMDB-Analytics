# Startup Check Specification

## Overview

On API startup, the back-end calls one startup procedure.
This procedure runs startup actions one by one in a fixed order.

Right now there is only one action, but the structure must allow adding more actions later.

## Goals

- Keep startup logic simple and explicit.
- Run actions deterministically, one after another.
- Make it easy to add new actions without changing startup architecture.

## Scope

This specification defines startup actions executed during API bootstrap.
It does not define endpoint contracts, front-end behavior, or data seeding workflows.

## Startup Procedure

The startup procedure is a single callable that:

1. Runs after database connections are initialized.
2. Iterates through a registered list of startup actions in deterministic order.
3. Executes each action sequentially.
4. Stops immediately if any action fails.

If all actions succeed, API startup continues.

## Action Module Rule

Each startup action must be implemented in a separate Python module.

Requirements for every action:

- Exposes one execute entrypoint (function or callable object).
- Is idempotent and safe to run on every startup.
- Raises an exception on failure.

## Current Action Set

### Action: ensure_duckdb_analytics_views

Purpose:
Create and maintain DuckDB analytical views used by API queries.

Preconditions:

- DuckDB connection is initialized.
- Source tables exist:
  - title_ratings
  - title_basics

Behavior:

- Creates or replaces the required views.
- Fails startup if source tables are missing or SQL execution fails.

#### View: top_rated_titles

```sql
CREATE OR REPLACE VIEW top_rated_titles AS
SELECT *
FROM (
  SELECT
    *,
    NTILE(5) OVER (ORDER BY averageRating DESC) AS bucket
  FROM title_ratings
  JOIN title_basics USING (tconst)
) t
WHERE bucket = 1;
```

#### View: most_popular_titles

```sql
CREATE OR REPLACE VIEW most_popular_titles AS
SELECT *
FROM (
  SELECT
    *,
    NTILE(5) OVER (ORDER BY numVotes DESC) AS bucket
  FROM title_ratings
  JOIN title_basics USING (tconst)
) t
WHERE bucket = 1;
```

#### View: top_rated_popular_titles

```sql
CREATE OR REPLACE VIEW top_rated_popular_titles AS
SELECT *
FROM (
  SELECT
    *,
    NTILE(5) OVER (ORDER BY averageRating DESC) AS rating_bucket,
    NTILE(5) OVER (ORDER BY numVotes DESC) AS popularity_bucket
  FROM title_ratings
  JOIN title_basics USING (tconst)
) t
WHERE rating_bucket = 1
  AND popularity_bucket = 1;
```

## Integration Points

- back-end/app/main.py
  - Lifespan startup calls the startup procedure.
- back-end/app/core
  - Contains the startup procedure and action modules.

## Extending With New Actions

To add a new startup action:

1. Create a new Python module for that action.
2. Implement one execute entrypoint in that module.
3. Register the action in the startup procedure action list.
4. Keep deterministic order explicit in the registration list.

No startup architecture changes are required.

## Acceptance Criteria

1. Startup calls one startup procedure.
2. Startup procedure executes actions sequentially in deterministic order.
3. Each action is implemented in a separate Python module.
4. Failure of an action stops startup.
5. ensure_duckdb_analytics_views is idempotent.
6. The three DuckDB views exist after startup when source tables exist.
7. A second action can be added by creating a new module and registering it.

## Out of Scope

- Endpoint-level filter semantics.
- Front-end usage details.
- Data seeding workflow.
- Runtime performance SLAs.
