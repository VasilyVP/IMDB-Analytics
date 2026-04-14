from __future__ import annotations

from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)


class SearchQueryParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    q: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            to_lower=True,
            min_length=3,
            max_length=20,
        ),
    ]
    limit: Annotated[int, Field(ge=1, le=50)] = 10
    top_rated: bool = Field(default=False, alias="topRated")
    most_popular: bool = Field(default=False, alias="mostPopular")
    min_rating: float | None = Field(default=None, alias="minRating", ge=1.0, le=10.0)
    max_rating: float | None = Field(default=None, alias="maxRating", ge=1.0, le=10.0)
    start_year_from: int | None = Field(default=None, alias="startYearFrom")
    start_year_to: int | None = Field(default=None, alias="startYearTo")
    genre: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, to_lower=True),
        ]
        | None
    ) = None
    title_type: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, to_lower=True),
        ]
        | None
    ) = Field(default=None, alias="titleType")

    @model_validator(mode="after")
    def _validate_ranges(self) -> SearchQueryParams:
        if (
            self.min_rating is not None
            and self.max_rating is not None
            and self.min_rating > self.max_rating
        ):
            raise ValueError("minRating must be less than or equal to maxRating")

        if (
            self.start_year_from is not None
            and self.start_year_to is not None
            and self.start_year_from > self.start_year_to
        ):
            raise ValueError("startYearFrom must be less than or equal to startYearTo")

        return self


class SearchResultItem(BaseModel):
    id: str
    name: str | None = None
    primaryTitle: str | None = None


class SearchResponse(BaseModel):
    results: list[SearchResultItem]
