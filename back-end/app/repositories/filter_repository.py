from __future__ import annotations

from typing import Literal

import duckdb

SourceRelation = Literal[
    "top_rated_titles",
    "most_popular_titles",
    "top_rated_popular_titles",
]

_GENRES_QUERY_TEMPLATE = """
SELECT DISTINCT genre
FROM (
    SELECT trim(g.genre) AS genre
    FROM {source_table} tb,
    unnest(string_split(tb.genres, ',')) AS g(genre)
    WHERE tb.genres IS NOT NULL
      AND tb.genres <> ''
      AND tb.genres <> '\\N'
) x
WHERE genre <> ''
ORDER BY genre
"""

_TITLE_TYPES_QUERY_TEMPLATE = """
SELECT DISTINCT titleType
FROM {source_table}
WHERE titleType IS NOT NULL
  AND titleType <> ''
  AND titleType <> '\\N'
ORDER BY titleType
"""

_YEAR_RANGE_QUERY_TEMPLATE = """
SELECT
    MIN(CAST(startYear AS INTEGER)) AS min_year,
    MAX(CAST(startYear AS INTEGER)) AS max_year
FROM {source_table}
WHERE startYear IS NOT NULL
"""

_RATING_RANGE_QUERY_TEMPLATE = """
SELECT
    MIN(averageRating) AS min_rating,
    MAX(averageRating) AS max_rating
FROM {source_table}
WHERE averageRating IS NOT NULL
"""


def _source_for_basics(source_relation: SourceRelation | None) -> str:
    if source_relation is None:
        return "title_basics"
    return source_relation


def _source_for_ratings(source_relation: SourceRelation | None) -> str:
    if source_relation is None:
        return "title_ratings"
    return source_relation


def _genres_query(source_relation: SourceRelation | None) -> str:
    return _GENRES_QUERY_TEMPLATE.format(source_table=_source_for_basics(source_relation))


def _title_types_query(source_relation: SourceRelation | None) -> str:
    return _TITLE_TYPES_QUERY_TEMPLATE.format(source_table=_source_for_basics(source_relation))


def _year_range_query(source_relation: SourceRelation | None) -> str:
    return _YEAR_RANGE_QUERY_TEMPLATE.format(source_table=_source_for_basics(source_relation))


def _rating_range_query(source_relation: SourceRelation | None) -> str:
    return _RATING_RANGE_QUERY_TEMPLATE.format(source_table=_source_for_ratings(source_relation))


def get_genres(
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_relation: SourceRelation | None = None,
) -> list[str]:
    rows = duckdb_conn.execute(_genres_query(source_relation)).fetchall()
    return [str(row[0]) for row in rows]


def get_title_types(
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_relation: SourceRelation | None = None,
) -> list[str]:
    rows = duckdb_conn.execute(_title_types_query(source_relation)).fetchall()
    return [str(row[0]) for row in rows]


def get_year_range(
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_relation: SourceRelation | None = None,
) -> tuple[int | None, int | None]:
    row = duckdb_conn.execute(_year_range_query(source_relation)).fetchone()
    if row is None:
        return (None, None)
    min_year = int(row[0]) if row[0] is not None else None
    max_year = int(row[1]) if row[1] is not None else None
    return (min_year, max_year)


def get_rating_range(
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_relation: SourceRelation | None = None,
) -> tuple[float | None, float | None]:
    row = duckdb_conn.execute(_rating_range_query(source_relation)).fetchone()
    if row is None:
        return (None, None)
    min_rating = float(row[0]) if row[0] is not None else None
    max_rating = float(row[1]) if row[1] is not None else None
    return (min_rating, max_rating)
