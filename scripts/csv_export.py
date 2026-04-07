"""
CSV export script
-----------------
Exports the 4 IMDB parquet files to CSV format.
Output is written to back-end/data/sources/.
Run `make seed` first if parquet files are missing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent
PARQUET_DIR = REPO_ROOT / "back-end" / "data"
CSV_DIR = PARQUET_DIR / "sources"

PARQUET_FILES: list[Path] = [
    PARQUET_DIR / "title.basics.parquet",
    PARQUET_DIR / "title.principals.parquet",
    PARQUET_DIR / "title.ratings.parquet",
    PARQUET_DIR / "name.unique.parquet",
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    missing = [p for p in PARQUET_FILES if not p.exists()]
    if missing:
        for p in missing:
            print(f"  Missing: {p.name}")
        print("\nRun `make seed` to generate the parquet files first.")
        sys.exit(1)

    CSV_DIR.mkdir(parents=True, exist_ok=True)

    print("Exporting parquet files to CSV ...")
    with duckdb.connect() as con:
        for pq_path in PARQUET_FILES:
            csv_path = CSV_DIR / pq_path.with_suffix(".csv").name
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{pq_path.as_posix()}'))"
                f" TO '{csv_path.as_posix()}' (FORMAT CSV, HEADER true)"
            )
            print(f"  {pq_path.name} → {csv_path.name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
