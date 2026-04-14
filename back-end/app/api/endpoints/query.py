from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app.api.dependencies import DuckDBDep
from app.schemas.filter_params import FilterParamsResponse
from app.schemas.search import SearchQueryParams, SearchResponse
from app.services import query_options_service, search_service

router = APIRouter()


@router.get("/filter-options", response_model=FilterParamsResponse)
def get_filter_options(
    duckdb: DuckDBDep,
    top_rated: Annotated[bool, Query(alias="topRated")] = False,
    most_popular: Annotated[bool, Query(alias="mostPopular")] = False,
) -> FilterParamsResponse:
    return query_options_service.get_filter_options(
        duckdb,
        top_rated=top_rated,
        most_popular=most_popular,
    )


@router.get("/search", response_model=SearchResponse, response_model_exclude_none=True)
def search(
    duckdb: DuckDBDep,
    params: Annotated[SearchQueryParams, Query()],
) -> SearchResponse:
    return search_service.search(duckdb, params)
