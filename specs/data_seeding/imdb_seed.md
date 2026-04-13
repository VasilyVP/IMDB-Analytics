# Spec: imdb_seed.py

## Purpose

Downloads IMDB dataset files, converts them to Parquet, and rebuilds the local DuckDB database (`back-end/data/imdb.duckdb`). Includes a staleness check — if all Parquet files are younger than 30 days the script skips re-download work but still rebuilds DuckDB from existing Parquet files.

---

## Usage

```bash
uv run python scripts/imdb_seed.py [--force]
```

| Flag      | Description                                      |
|-----------|--------------------------------------------------|
| `--force` | Skip the staleness check and always re-download  |

Requires write access to `back-end/data/`.

---

## Configuration

| Constant          | Value                              | Description                                      |
|-------------------|------------------------------------|--------------------------------------------------|
| `PARQUET_DIR`     | `back-end/data/`                   | Output directory for Parquet files and DuckDB    |
| `SOURCES_DIR`     | `back-end/data/sources/`           | Temporary directory for downloaded `.gz`/`.tsv`  |
| `DUCKDB_PATH`     | `back-end/data/imdb.duckdb`        | Target DuckDB database file                      |
| `STALENESS_DAYS`  | `30`                               | Max age (days) before Parquet files are re-built |

---

## Source Datasets

All files are downloaded from `https://datasets.imdbws.com/`.

| URL filename                  | Output Parquet file              | DuckDB table name    |
|-------------------------------|----------------------------------|----------------------|
| `title.basics.tsv.gz`         | `title.basics.parquet`           | `title_basics`       |
| `title.principals.tsv.gz`     | `title.principals.parquet`       | `title_principals`   |
| `title.ratings.tsv.gz`        | `title.ratings.parquet`          | `title_ratings`      |
| `name.basics.tsv.gz`          | `name.unique.parquet`            | `name_unique`        |

Processing order is fixed: `title.basics` → `title.principals` → `title.ratings` → `name.basics`. The `name.basics` dataset depends on the earlier two being written first (see Filtering section below).

---

## Execution Phases

1. **Staleness check** — if all four Parquet files exist and are younger than `STALENESS_DAYS`, skip phases 2-5 and proceed to phase 6.
2. **Report stale files** — list each missing or outdated Parquet file with its reason (`missing` or `older than 30 days`).
3. **Download** — download each `.tsv.gz` file sequentially to `SOURCES_DIR`, printing progress as percentage and MB.
4. **Decompress** — decompress each `.gz` file to a `.tsv` file in the same directory.
5. **Convert to Parquet** — convert each `.tsv` to Parquet using an in-memory DuckDB connection; IMDB null sentinel `\N` is mapped to SQL `NULL`.
6. **Rebuild DuckDB** — delete `imdb.duckdb` if it exists, then create all four tables from the Parquet files.
7. **Cleanup** — delete all `.gz` and `.tsv` files from `SOURCES_DIR`.

---

## Parquet Conversion Details

### General datasets (`title.principals`, `title.ratings`)

All columns are read as-is from the TSV with `\N` replaced by `NULL`. No row filtering is applied.

### `title.basics`

All columns are read from the TSV with `\N` replaced by `NULL`, but rows are filtered to exclude:

- `titleType = 'tvEpisode'`
- `titleType = 'videoGame'`

Equivalent filter condition:

```sql
WHERE titleType NOT IN ('tvEpisode', 'videoGame')
```

### `name.unique` (filtered from `name.basics`)

Only person records that have at least one associated title are retained. The filter is a semi-join through `title.principals`:

```sql
SELECT nb.*
FROM read_csv('name.basics.tsv', delim='\t', header=true, nullstr='\N') AS nb
WHERE nb.nconst IN (
    SELECT DISTINCT tp.nconst
    FROM read_parquet('title.principals.parquet') AS tp
    INNER JOIN read_parquet('title.basics.parquet') AS tb
        ON tp.tconst = tb.tconst
)
```

This is why `title.basics.parquet` and `title.principals.parquet` must be written before `name.unique.parquet`.

---

## DuckDB Rebuild

DuckDB is rebuilt by deleting the database file first (if present), then creating tables from Parquet:

```sql
CREATE TABLE <table> AS SELECT * FROM read_parquet('<path>');
```

Tables are rebuilt in the order: `title_basics` → `title_principals` → `title_ratings` → `name_unique`.

DuckDB holds an exclusive lock on `imdb.duckdb` during this phase — no concurrent queries.

---

## Output Files

| File path                            | Description                          |
|--------------------------------------|--------------------------------------|
| `back-end/data/title.basics.parquet`     | Title records excluding `tvEpisode` and `videoGame` |
| `back-end/data/title.principals.parquet` | Title-person principal records       |
| `back-end/data/title.ratings.parquet`    | Title rating records                 |
| `back-end/data/name.unique.parquet`      | Person records linked to ≥1 title    |
| `back-end/data/imdb.duckdb`              | DuckDB database with all four tables |

Intermediate `.gz` and `.tsv` files in `back-end/data/sources/` are removed after a successful run.

---

## Staleness Logic

A Parquet file is considered **fresh** if it exists and its modification time is less than 30 days ago (UTC). If **all** Parquet files are fresh, the script skips download/decompression/conversion work and still rebuilds DuckDB from those Parquet files. If **any** file is stale or missing, all datasets are re-downloaded and all Parquet files are regenerated before DuckDB is rebuilt.

Passing `--force` bypasses this check entirely — the pipeline always runs regardless of file age.
