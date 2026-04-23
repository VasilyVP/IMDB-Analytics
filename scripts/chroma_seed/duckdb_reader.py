from __future__ import annotations

from typing import Any

import duckdb

from .models import PersonRecord, TitleRecord

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

_COUNT_ELIGIBLE_TITLE_QUERY = """
SELECT COUNT(*)
FROM title_basics tb
INNER JOIN title_ratings tr ON tr.tconst = tb.tconst
WHERE tb.titleType = 'movie'
  AND tr.averageRating > 7.5
  AND tb.startYear > 2013
  AND tb.startYear < 2024
  {after_filter}
"""

_ELIGIBLE_PERSON_QUERY = """
SELECT DISTINCT
    nu.nconst,
    nu.primaryName,
    nu.birthYear,
    (
        SELECT GROUP_CONCAT(DISTINCT category, ',')
        FROM title_principals tp2
        WHERE nu.nconst = tp2.nconst AND tp2.category IN ('actor', 'director')
    ) AS category
FROM title_basics tb
INNER JOIN title_ratings tr USING (tconst)
INNER JOIN title_principals tp USING (tconst)
INNER JOIN name_unique nu USING (nconst)
WHERE tr.averageRating > 7.5
    AND tb.titleType = 'movie'
    AND tb.startYear > 2013
    AND tb.startYear < 2024
    AND (tp.category = 'actor' OR tp.category = 'director')
    AND nu.birthYear IS NOT NULL
    {after_filter}
ORDER BY nconst
LIMIT ?
"""

_COUNT_ELIGIBLE_PERSON_QUERY = """
SELECT COUNT(*)
FROM (
    SELECT DISTINCT nu.nconst
    FROM title_basics tb
    INNER JOIN title_ratings tr USING (tconst)
    INNER JOIN title_principals tp USING (tconst)
    INNER JOIN name_unique nu USING (nconst)
    WHERE tb.titleType = 'movie'
        AND tr.averageRating > 7.5
        AND tb.startYear > 2013
        AND tb.startYear < 2024
        AND (tp.category = 'actor' OR tp.category = 'director')
        AND nu.birthYear IS NOT NULL
        {after_filter}
) eligible_persons
"""


def count_eligible_titles(
    connection: duckdb.DuckDBPyConnection,
    after_title_id: str | None = None,
) -> int:
    after_filter, params = _build_after_filter(after_title_id)
    query = _COUNT_ELIGIBLE_TITLE_QUERY.format(after_filter=after_filter)
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


def count_eligible_persons(
    connection: duckdb.DuckDBPyConnection,
    after_person_id: str | None = None,
) -> int:
    after_filter, params = _build_after_person_filter(after_person_id)
    query = _COUNT_ELIGIBLE_PERSON_QUERY.format(after_filter=after_filter)
    row = connection.execute(query, params).fetchone()
    return int(row[0]) if row is not None else 0


def fetch_person_batch(
    connection: duckdb.DuckDBPyConnection,
    batch_size: int,
    after_person_id: str | None,
) -> list[PersonRecord]:
    after_filter, params = _build_after_person_filter(after_person_id)
    query = _ELIGIBLE_PERSON_QUERY.format(after_filter=after_filter)
    result = connection.execute(query, [*params, batch_size]).fetchall()
    return [
        PersonRecord(
            person_id=row[0],
            name=row[1],
            birth_year=int(row[2]) if row[2] is not None else None,
            category=row[3],
        )
        for row in result
    ]


def _build_after_filter(after_title_id: str | None) -> tuple[str, list[Any]]:
    if after_title_id is None:
        return "", []
    return "AND tb.tconst > ?", [after_title_id]


def _build_after_person_filter(after_person_id: str | None) -> tuple[str, list[Any]]:
    if after_person_id is None:
        return "", []
    return "AND nu.nconst > ?", [after_person_id]
