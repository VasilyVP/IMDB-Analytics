from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ItemDetailsParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    title_id: str | None = Field(default=None, alias="titleId", pattern=r"^tt[0-9]+$")
    name_id: str | None = Field(default=None, alias="nameId", pattern=r"^nm[0-9]+$")

    @model_validator(mode="after")
    def _validate_exactly_one_identifier(self) -> ItemDetailsParams:
        has_title_id = self.title_id is not None
        has_name_id = self.name_id is not None
        if has_title_id == has_name_id:
            raise ValueError("Exactly one of titleId or nameId must be provided")
        return self


class ItemDetailsResponse(BaseModel):
    id: str
    entityType: str
    description: str
