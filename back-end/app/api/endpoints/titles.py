from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app.api.dependencies import DuckDBDep
from app.schemas.filter_options import FilterOptionsResponse
from app.services import filter_service

router = APIRouter()


@router.get("/filters", response_model=FilterOptionsResponse)
def get_title_filters(
    duckdb: DuckDBDep,
    top_rated: Annotated[bool, Query(alias="topRated")] = False,
    most_popular: Annotated[bool, Query(alias="mostPopular")] = False,
) -> FilterOptionsResponse:
    return filter_service.get_filter_options(
        duckdb,
        top_rated=top_rated,
        most_popular=most_popular,
    )
