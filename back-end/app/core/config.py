from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).parent.parent.parent.parent
_ENV_FILE = str(_REPO_ROOT / ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    NEO4J_URI: str
    NEO4J_USER: str
    NEO4J_PASSWORD: str
    DUCKDB_PATH: str = "data/imdb.duckdb"

    @field_validator("NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "DUCKDB_PATH")
    @classmethod
    def _must_not_be_empty(cls, v: str, info: ValidationInfo) -> str:
        if not v.strip():
            raise ValueError(f"{info.field_name} must not be empty")
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
