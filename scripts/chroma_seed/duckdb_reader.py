from __future__ import annotations

from typing import Any

import duckdb

from .models import TitleRecord

_ELIGIBLE_TITLE_QUERY = """
SELECT tb.tconst, tb.primaryTitle, tb.startYear
FROM title_basics tb
INNER JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.titleType = 'movie'
  AND tr.averageRating > 7.5
  AND tb.startYear > 2013
  AND tb.startYear < 2024
  {after_filter}
ORDER BY tb.tconst
LIMIT ?
"""

_COUNT_ELIGIBLE_QUERY = """
SELECT COUNT(*)
FROM title_basics tb
INNER JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.titleType = 'movie'
  AND tr.averageRating > 7.5
  AND tb.startYear > 2013
  AND tb.startYear < 2024
  {after_filter}
"""


def count_eligible_titles(
    connection: duckdb.DuckDBPyConnection,
    after_title_id: str | None = None,
) -> int:
    after_filter, params = _build_after_filter(after_title_id)
    query = _COUNT_ELIGIBLE_QUERY.format(after_filter=after_filter)
    row = connection.execute(query, params).fetchone()
    return int(row[0]) if row is not None else 0


def fetch_title_batch(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int,
    after_title_id: str | None,
) -> list[TitleRecord]:
    after_filter, params = _build_after_filter(after_title_id)
    query = _ELIGIBLE_TITLE_QUERY.format(after_filter=after_filter)
    result = connection.execute(query, [*params, batch_size]).fetchall()
    return [
        TitleRecord(title_id=row[0], title=row[1], start_year=int(row[2])) for row in result
    ]


def _build_after_filter(after_title_id: str | None) -> tuple[str, list[Any]]:
    if after_title_id is None:
        return "", []
    return "AND tb.tconst > ?", [after_title_id]
