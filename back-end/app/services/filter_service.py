from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import datetime
from functools import lru_cache
import re

import duckdb

from app.repositories import filter_repository
from app.schemas.filter_options import (
    FilterOptionsResponse,
    NumericRangeFloat,
    NumericRangeInt,
    TitleTypeOption,
)

_TITLE_TYPE_LABELS: dict[str, str] = {
    "movie": "Movie",
    "tvSeries": "TV Series",
}

_CAMEL_CASE_BOUNDARY = re.compile(r"([a-z])([A-Z])")


def _format_title_type_label(value: str) -> str:
    if value in _TITLE_TYPE_LABELS:
        return _TITLE_TYPE_LABELS[value]

    normalized = value.replace("_", " ").replace("-", " ")
    normalized = _CAMEL_CASE_BOUNDARY.sub(r"\1 \2", normalized)
    return " ".join(part.capitalize() for part in normalized.split())


def _cursor_or_self(duckdb_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    if hasattr(duckdb_conn, "cursor"):
        return duckdb_conn.cursor()
    return duckdb_conn


def _resolve_source_relation(
    top_rated: bool,
    most_popular: bool,
) -> filter_repository.SourceRelation | None:
    if top_rated and most_popular:
        return "top_rated_popular_titles"
    if top_rated:
        return "top_rated_titles"
    if most_popular:
        return "most_popular_titles"
    return None


@lru_cache(maxsize=8)
def get_filter_options(
    duckdb_conn: duckdb.DuckDBPyConnection,
    top_rated: bool = False,
    most_popular: bool = False,
) -> FilterOptionsResponse:
    source_relation = _resolve_source_relation(top_rated, most_popular)

    # These lookups are independent and can be fetched concurrently.
    with ThreadPoolExecutor(max_workers=4) as executor:
        genres_future = executor.submit(
            filter_repository.get_genres,
            _cursor_or_self(duckdb_conn),
            source_relation,
        )
        title_types_future = executor.submit(
            filter_repository.get_title_types,
            _cursor_or_self(duckdb_conn),
            source_relation,
        )
        year_range_future = executor.submit(
            filter_repository.get_year_range,
            _cursor_or_self(duckdb_conn),
            source_relation,
        )
        rating_range_future = executor.submit(
            filter_repository.get_rating_range,
            _cursor_or_self(duckdb_conn),
            source_relation,
        )

        genres = genres_future.result()
        title_types = title_types_future.result()
        min_year, max_year = year_range_future.result()
        min_rating, max_rating = rating_range_future.result()

    if max_year is not None:
        max_year = min(max_year, datetime.date.today().year + 5)

    return FilterOptionsResponse(
        genres=genres,
        titleTypes=[
            TitleTypeOption(value=value, label=_format_title_type_label(value))
            for value in title_types
        ],
        yearRange=NumericRangeInt(min=min_year, max=max_year),
        ratingRange=NumericRangeFloat(min=min_rating, max=max_rating),
    )
