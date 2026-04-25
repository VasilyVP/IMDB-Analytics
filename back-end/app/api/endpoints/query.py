from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request
from neo4j.exceptions import AuthError, Neo4jError, ServiceUnavailable

from app.api.dependencies import DuckDBDep
from app.core.limiter import limiter
from app.schemas.filter_params import FilterParamsResponse
from app.schemas.graph_data import GraphDataParams, GraphDataResponse
from app.schemas.item_details import ItemDetailsParams, ItemDetailsResponse
from app.schemas.items_found import ItemsFoundParams, ItemsFoundResponse
from app.schemas.search import SearchQueryParams, SearchResponse
from app.services import (
    graph_data_service,
    item_details_service,
    items_found_service,
    query_options_service,
    search_service,
)

router = APIRouter()


@router.get("/filter-options", response_model=FilterParamsResponse)
@limiter.limit("2/second")  # type: ignore[misc]
def get_filter_options(
    request: Request,
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
@limiter.limit("3/second")  # type: ignore[misc]
def search(
    request: Request,
    duckdb: DuckDBDep,
    params: Annotated[SearchQueryParams, Query()],
) -> SearchResponse:
    return search_service.search(duckdb, params)


@router.get("/items-found", response_model=ItemsFoundResponse)
@limiter.limit("2/second")  # type: ignore[misc]
def get_items_found(
    request: Request,
    duckdb: DuckDBDep,
    params: Annotated[ItemsFoundParams, Query()],
) -> ItemsFoundResponse:
    return items_found_service.get_items_found(duckdb, params)


@router.get("/item-details", response_model=ItemDetailsResponse)
@limiter.limit("3/second")  # type: ignore[misc]
def get_item_details(
    request: Request,
    duckdb: DuckDBDep,
    params: Annotated[ItemDetailsParams, Query()],
) -> ItemDetailsResponse:
    try:
        return item_details_service.get_item_details(duckdb, params)
    except item_details_service.ItemDetailsNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Description not found") from exc
    except item_details_service.ItemDetailsUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail="Description service is temporarily unavailable",
        ) from exc


@router.get("/graph-data", response_model=GraphDataResponse)
@limiter.limit("1/second")  # type: ignore[misc]
def get_graph_data(
    request: Request,
    duckdb: DuckDBDep,
    params: Annotated[GraphDataParams, Query()],
) -> GraphDataResponse:
    try:
        return graph_data_service.get_graph_data(duckdb, params)
    except (Neo4jError, ServiceUnavailable, AuthError, OSError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Graph database is temporarily unavailable",
        ) from exc
