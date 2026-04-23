from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SeedMode = Literal["titles", "persons"]


@dataclass(frozen=True, slots=True)
class TitleRecord:
    title_id: str
    title: str
    start_year: int


@dataclass(frozen=True, slots=True)
class PersonRecord:
    person_id: str
    name: str
    birth_year: int | None
    category: str


@dataclass(frozen=True, slots=True)
class ChromaSeedRecord:
    title_id: str
    title: str
    start_year: int
    human_description: str
    embedding_description: str


@dataclass(frozen=True, slots=True)
class ChromaPersonSeedRecord:
    person_id: str
    name: str
    birth_year: int | None
    category: str
    human_description: str
    embedding_description: str


@dataclass(frozen=True, slots=True)
class SummaryCounts:
    success_count: int
    failed_count: int


@dataclass(frozen=True, slots=True)
class RuntimeStats:
    processed_count: int
    total_count: int
    success_count: int
    failed_count: int
    elapsed_seconds: float
