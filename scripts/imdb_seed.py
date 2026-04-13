"""
IMDB seed script
----------------
- Checks if parquet files are missing or older than 30 days.
- Downloads IMDB source .tsv.gz files sequentially.
- Decompresses to .tsv.
- Converts to parquet via DuckDB (\\N → NULL, names filtered to those
  with matching titles in title.basics via title.principals join).
- Rebuilds imdb.duckdb from the parquet files.
- Removes all intermediate .gz and .tsv files.
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# IMDB datasets URLs
# ---------------------------------------------------------------------------
name_basics_tsv_gz_url = "https://datasets.imdbws.com/name.basics.tsv.gz"
title_basics_tsv_gz_url = "https://datasets.imdbws.com/title.basics.tsv.gz"
title_principals_tsv_gz_url = "https://datasets.imdbws.com/title.principals.tsv.gz"
title_ratings_tsv_gz_url = "https://datasets.imdbws.com/title.ratings.tsv.gz"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
PARQUET_DIR = REPO_ROOT / "back-end" / "data"
SOURCES_DIR = PARQUET_DIR / "sources"
DUCKDB_PATH = PARQUET_DIR / "imdb.duckdb"

# Parquet file paths
PQ_TITLE_BASICS = PARQUET_DIR / "title.basics.parquet"
PQ_TITLE_PRINCIPALS = PARQUET_DIR / "title.principals.parquet"
PQ_TITLE_RATINGS = PARQUET_DIR / "title.ratings.parquet"
PQ_NAME_UNIQUE = PARQUET_DIR / "name.unique.parquet"

# Table name → parquet path (for DuckDB rebuild); name.basics last so the
# filtered parquet can reference already-written title.basics & title.principals.
DATASETS: list[tuple[str, Path]] = [
    (title_basics_tsv_gz_url, PQ_TITLE_BASICS),
    (title_principals_tsv_gz_url, PQ_TITLE_PRINCIPALS),
    (title_ratings_tsv_gz_url, PQ_TITLE_RATINGS),
    (name_basics_tsv_gz_url, PQ_NAME_UNIQUE),
]

# Table name → parquet path for DuckDB rebuild
DUCKDB_TABLES: list[tuple[str, Path]] = [
    ("title_basics", PQ_TITLE_BASICS),
    ("title_principals", PQ_TITLE_PRINCIPALS),
    ("title_ratings", PQ_TITLE_RATINGS),
    ("name_unique", PQ_NAME_UNIQUE),
]

STALENESS_DAYS = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parquet_files() -> list[Path]:
    return [pq for _, pq in DATASETS]


def _is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return datetime.now(tz=timezone.utc) - mtime < timedelta(days=STALENESS_DAYS)


def _all_fresh() -> bool:
    return all(_is_fresh(p) for p in _parquet_files())


def _download(url: str, dest: Path) -> None:
    filename = dest.name
    print(f"  Downloading {filename} ...", flush=True)
    downloaded = 0

    def _reporthook(block_num: int, block_size: int, total_size: int) -> None:
        nonlocal downloaded
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 // total_size)
            mb = downloaded / 1_048_576
            print(f"\r    {pct:3d}%  {mb:.1f} MB", end="", flush=True)
        else:
            mb = downloaded / 1_048_576
            print(f"\r    {mb:.1f} MB downloaded", end="", flush=True)

    urllib.request.urlretrieve(url, dest, reporthook=_reporthook)
    print(f"\r    Done — {downloaded / 1_048_576:.1f} MB", flush=True)


def _decompress(gz_path: Path) -> Path:
    tsv_path = gz_path.with_suffix("")  # removes .gz, leaving .tsv
    print(f"  Decompressing {gz_path.name} → {tsv_path.name} ...", flush=True)
    with gzip.open(gz_path, "rb") as f_in, open(tsv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return tsv_path


def _tsv_to_parquet(
    con: duckdb.DuckDBPyConnection, tsv_path: Path, parquet_path: Path
) -> None:
    tsv = tsv_path.as_posix()
    pq = parquet_path.as_posix()
    tb_pq = PQ_TITLE_BASICS.as_posix()
    tp_pq = PQ_TITLE_PRINCIPALS.as_posix()

    if parquet_path == PQ_NAME_UNIQUE:
        # Keep only names that have associated titles via title.principals
        sql = f"""
            COPY (
                SELECT nb.*
                FROM read_csv('{tsv}', delim='\\t', header=true, nullstr='\\N') AS nb
                WHERE nb.nconst IN (
                    SELECT DISTINCT tp.nconst
                    FROM read_parquet('{tp_pq}') AS tp
                    INNER JOIN read_parquet('{tb_pq}') AS tb
                        ON tp.tconst = tb.tconst
                )
            ) TO '{pq}' (FORMAT PARQUET)
        """
    elif parquet_path == PQ_TITLE_BASICS:
        sql = f"""
            COPY (
                SELECT *
                FROM read_csv('{tsv}', delim='\\t', header=true, nullstr='\\N')
                WHERE titleType NOT IN ('tvEpisode', 'videoGame')
            ) TO '{pq}' (FORMAT PARQUET)
        """
    else:
        sql = f"""
            COPY (
                SELECT *
                FROM read_csv('{tsv}', delim='\\t', header=true, nullstr='\\N')
            ) TO '{pq}' (FORMAT PARQUET)
        """

    print(f"  Converting {tsv_path.name} → {parquet_path.name} ...", flush=True)
    con.execute(sql)


def _rebuild_duckdb() -> None:
    print(f"\nRebuilding {DUCKDB_PATH.name} ...", flush=True)
    DUCKDB_PATH.unlink(missing_ok=True)
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        for table, pq_path in DUCKDB_TABLES:
            con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{pq_path.as_posix()}')"
            )
            print(f"  Created table: {table}", flush=True)


def _cleanup_sources() -> None:
    print("\nCleaning up source files ...", flush=True)
    for pattern in ("*.gz", "*.tsv"):
        for f in SOURCES_DIR.glob(pattern):
            f.unlink()
            print(f"  Removed {f.name}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed IMDB data into Parquet and DuckDB."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the staleness check and always re-download",
    )
    args = parser.parse_args()

    needs_download = args.force or not _all_fresh()

    if not needs_download:
        print(
            "Parquet files are up to date (all younger than 30 days). Skipping download."
        )
    else:
        stale = [p for p in _parquet_files() if not _is_fresh(p)]
        for p in stale:
            reason = "missing" if not p.exists() else "older than 30 days"
            print(f"  {p.name}: {reason}")

        SOURCES_DIR.mkdir(parents=True, exist_ok=True)
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        # Phase 2: download sequentially
        print("\nDownloading source files ...")
        gz_files: list[Path] = []
        for url, _ in DATASETS:
            filename = url.rsplit("/", 1)[-1]
            dest = SOURCES_DIR / filename
            _download(url, dest)
            gz_files.append(dest)

        # Phase 3: decompress
        print("\nDecompressing ...")
        tsv_files: list[Path] = []
        for gz_path in gz_files:
            tsv_files.append(_decompress(gz_path))

        # Phase 4: convert to parquet (in-memory DuckDB connection)
        print("\nConverting to parquet ...")
        with duckdb.connect() as con:
            for (_, parquet_path), tsv_path in zip(DATASETS, tsv_files):
                _tsv_to_parquet(con, tsv_path, parquet_path)

    # Phase 5: rebuild imdb.duckdb
    _rebuild_duckdb()

    # Phase 6: remove .gz and .tsv files
    _cleanup_sources()

    print("\nDone.")


if __name__ == "__main__":
    main()
