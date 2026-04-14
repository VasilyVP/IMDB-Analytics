from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import duckdb

SourceRelation = Literal[
    "top_rated_titles",
    "most_popular_titles",
    "top_rated_popular_titles",
]


@dataclass(frozen=True)
class SearchRow:
    id: str
    result: str
    title_type: str  # '_' for persons


_VALID_SOURCE_RELATIONS: frozenset[str] = frozenset(
    {"top_rated_titles", "most_popular_titles", "top_rated_popular_titles"}
)


def search(
    duckdb_conn: duckdb.DuckDBPyConnection,
    query: str,
    limit: int,
    source_relation: SourceRelation | None,
    min_rating: float | None,
    max_rating: float | None,
    start_year_from: int | None,
    start_year_to: int | None,
    genre: str | None,
    title_type: str | None,
) -> list[SearchRow]:
    contains_pattern = query if "%" in query else "%" + "%".join(query.split()) + "%"

    filter_clauses: list[str] = []
    filter_params: list[object] = []

    if min_rating is not None:
        filter_clauses.append("tr.averageRating >= ?")
        filter_params.append(min_rating)

    if max_rating is not None:
        filter_clauses.append("tr.averageRating <= ?")
        filter_params.append(max_rating)

    if start_year_from is not None:
        filter_clauses.append("CAST(tb.startYear AS INTEGER) >= ?")
        filter_params.append(start_year_from)

    if start_year_to is not None:
        filter_clauses.append("CAST(tb.startYear AS INTEGER) <= ?")
        filter_params.append(start_year_to)

    if genre is not None:
        filter_clauses.append(
            """EXISTS (
                SELECT 1
                FROM unnest(string_split(COALESCE(tb.genres, ''), ',')) AS g(genre)
                WHERE lower(trim(g.genre)) = ?
            )"""
        )
        filter_params.append(genre)

    if title_type is not None:
        filter_clauses.append("lower(tb.titleType) = ?")
        filter_params.append(title_type)

    if source_relation is not None:
        if source_relation not in _VALID_SOURCE_RELATIONS:
            raise ValueError(f"Invalid source_relation: {source_relation!r}")
        filter_clauses.append(f"tb.tconst IN (SELECT tconst FROM {source_relation})")

    title_where = "tb.primaryTitle ILIKE ?"
    person_where = "primaryName ILIKE ?"
    if filter_clauses:
        joined_clauses = " AND ".join(clause.strip() for clause in filter_clauses)
        title_where += " AND " + joined_clauses
        person_where += f""" AND EXISTS (
                SELECT 1
                FROM title_principals tp
                JOIN title_basics tb ON tp.tconst = tb.tconst
                LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
                WHERE tp.nconst = name_unique.nconst
                AND {joined_clauses}
            )"""

    sql = f"""
        SELECT id, result, title_type
        FROM (
            SELECT nconst AS id, primaryName AS result, '_' AS title_type
            FROM name_unique
            WHERE {person_where}
            UNION
            SELECT tconst AS id, tb.primaryTitle AS result, tb.titleType AS title_type
            FROM title_basics tb
            LEFT JOIN title_ratings tr USING (tconst)
            WHERE {title_where}
        )
        ORDER BY title_type
        LIMIT ?
    """

    params: list[object] = [
        contains_pattern,
        *filter_params,
        contains_pattern,
        *filter_params,
        limit,
    ]

    rows = duckdb_conn.execute(sql, params).fetchall()
    return [
        SearchRow(
            id=str(row[0]),
            result=str(row[1]),
            title_type=str(row[2]),
        )
        for row in rows
    ]
