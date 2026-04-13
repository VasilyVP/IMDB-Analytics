from __future__ import annotations
import duckdb
from app.core.common import logger


def execute(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("Ensuring DuckDB analytics views exist...")

    _ensure_required_tables_exist(duckdb_conn)

    duckdb_conn.execute(
        """
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
        """
    )

    duckdb_conn.execute(
        """
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
        """
    )

    duckdb_conn.execute(
        """
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
        """
    )


def _ensure_required_tables_exist(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    required_tables = ("title_ratings", "title_basics")

    for table_name in required_tables:
        try:
            duckdb_conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1;")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Required DuckDB table is missing or unreadable: {table_name}"
            ) from exc
