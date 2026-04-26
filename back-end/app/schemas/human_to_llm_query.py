from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

QueryType = Literal[
    "person_search",
    "film_search",
    "person",
    "film",
    "recommendation",
]
RoleType = Literal["actor", "director"]


class HumanToLlmQueryRequest(BaseModel):
    query: str
    limit: int = Field(default=10, ge=1, le=50)

    @field_validator("query", mode="before")
    @classmethod
    def _normalize_query(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("query must be a string")
        normalized = value.strip()
        if normalized == "":
            raise ValueError("query must not be empty")
        return normalized


class HumanToLlmParsedFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: RoleType | None = None
    name: str | None = None
    title: str | None = None
    details: str | None = None

    @field_validator("name", "title", "details", mode="before")
    @classmethod
    def _validate_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        normalized = value.strip()
        if normalized == "":
            raise ValueError("value must not be empty")
        return normalized

    @field_validator("details")
    @classmethod
    def _validate_details_word_count(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if len(value.split()) > 20:
            raise ValueError("details must contain at most 20 words")
        return value


class ClassifiedQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: QueryType
    parsed: HumanToLlmParsedFields = Field(
        validation_alias=AliasChoices("parsed", "details"),
    )


class HumanToLlmResultItem(BaseModel):
    id: str
    label: str
    entityType: Literal["person", "title"]
    birthYear: int | None
    startYear: int | None
    score: float | None = None


class HumanToLlmQueryResponse(BaseModel):
    type: QueryType
    parsed: HumanToLlmParsedFields
    results: list[HumanToLlmResultItem]
