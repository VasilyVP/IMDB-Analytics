from __future__ import annotations

import duckdb

from app.repositories import search_repository
from app.schemas.search import SearchQueryParams, SearchResponse, SearchResultItem


def _resolve_source_relation(
    top_rated: bool,
    most_popular: bool,
) -> search_repository.SourceRelation | None:
    if top_rated and most_popular:
        return "top_rated_popular_titles"
    if top_rated:
        return "top_rated_titles"
    if most_popular:
        return "most_popular_titles"
    return None


def search(
    duckdb_conn: duckdb.DuckDBPyConnection,
    params: SearchQueryParams,
) -> SearchResponse:
    normalized_query = params.q
    limit = params.limit
    source_relation = _resolve_source_relation(params.top_rated, params.most_popular)

    rows = search_repository.search(
        duckdb_conn,
        query=normalized_query,
        limit=limit,
        source_relation=source_relation,
        min_rating=params.min_rating,
        max_rating=params.max_rating,
        start_year_from=params.start_year_from,
        start_year_to=params.start_year_to,
        genre=params.genre,
        title_type=params.title_type,
    )

    return SearchResponse(
        results=[
            SearchResultItem(
                id=row.id,
                name=row.result if row.title_type == "_" else None,
                primaryTitle=row.result if row.title_type != "_" else None,
            )
            for row in rows
        ]
    )
