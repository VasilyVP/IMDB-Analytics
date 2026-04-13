from __future__ import annotations

from pydantic import BaseModel


class TitleTypeOption(BaseModel):
    value: str
    label: str


class NumericRangeInt(BaseModel):
    min: int | None
    max: int | None


class NumericRangeFloat(BaseModel):
    min: float | None
    max: float | None


class FilterOptionsResponse(BaseModel):
    genres: list[str]
    titleTypes: list[TitleTypeOption]
    yearRange: NumericRangeInt
    ratingRange: NumericRangeFloat
