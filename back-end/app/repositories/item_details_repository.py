from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import duckdb

from app.core.config import get_settings

_TITLE_PROMPT_INPUT_QUERY = """
SELECT tb.tconst, tb.primaryTitle, tb.startYear
FROM title_basics tb
WHERE tb.tconst = ?
LIMIT 1
"""

_PERSON_PROMPT_INPUT_QUERY = """
SELECT
    nu.nconst,
    nu.primaryName,
    nu.birthYear,
    COALESCE(
        (
            SELECT GROUP_CONCAT(DISTINCT tp.category, ',')
            FROM title_principals tp
            WHERE tp.nconst = nu.nconst
              AND tp.category IN ('actor', 'director')
        ),
        'person'
    )
FROM name_unique nu
WHERE nu.nconst = ?
LIMIT 1
"""


@dataclass(frozen=True, slots=True)
class TitlePromptInput:
    title_id: str
    title: str
    start_year: int


@dataclass(frozen=True, slots=True)
class PersonPromptInput:
    person_id: str
    name: str
    birth_year: int | None
    category: str


def fetch_title_prompt_input(
    duckdb_conn: duckdb.DuckDBPyConnection,
    title_id: str,
) -> TitlePromptInput | None:
    row = duckdb_conn.execute(_TITLE_PROMPT_INPUT_QUERY, [title_id]).fetchone()
    if row is None:
        return None

    start_year = int(row[2]) if row[2] is not None else 0
    return TitlePromptInput(
        title_id=row[0],
        title=row[1],
        start_year=start_year,
    )


def fetch_person_prompt_input(
    duckdb_conn: duckdb.DuckDBPyConnection,
    name_id: str,
) -> PersonPromptInput | None:
    row = duckdb_conn.execute(_PERSON_PROMPT_INPUT_QUERY, [name_id]).fetchone()
    if row is None:
        return None

    return PersonPromptInput(
        person_id=row[0],
        name=row[1],
        birth_year=int(row[2]) if row[2] is not None else None,
        category=row[3],
    )


def get_title_description(title_id: str) -> str | None:
    settings = get_settings()
    collection = _get_collection(settings.CHROMA_COLLECTION_TITLES)

    return (
        _get_human_description_by_where(collection, {"titleId": title_id})
        or _get_human_description_by_where(collection, {"id": title_id})
        or _get_human_description_by_id(collection, title_id)
    )


def get_person_description(name_id: str) -> str | None:
    settings = get_settings()
    collection = _get_collection(settings.CHROMA_COLLECTION_PERSONS)

    return (
        _get_human_description_by_where(collection, {"personId": name_id})
        or _get_human_description_by_where(collection, {"id": name_id})
        or _get_human_description_by_id(collection, name_id)
    )


def upsert_title_description(
    *,
    title_id: str,
    title: str,
    start_year: int,
    human_description: str,
    embedding_description: str,
) -> None:
    settings = get_settings()
    collection = _get_collection(settings.CHROMA_COLLECTION_TITLES)

    collection.upsert(
        ids=[title_id],
        documents=[embedding_description],
        metadatas=[
            {
                "titleId": title_id,
                "title": title,
                "startYear": start_year,
                "human_description": human_description,
            }
        ],
    )


def upsert_person_description(
    *,
    person_id: str,
    name: str,
    birth_year: int | None,
    category: str,
    human_description: str,
    embedding_description: str,
) -> None:
    settings = get_settings()
    collection = _get_collection(settings.CHROMA_COLLECTION_PERSONS)

    collection.upsert(
        ids=[person_id],
        documents=[embedding_description],
        metadatas=[
            _without_none_values(
                {
                    "personId": person_id,
                    "name": name,
                    "birthYear": birth_year,
                    "category": category,
                    "human_description": human_description,
                }
            )
        ],
    )


def _get_collection(collection_name: str) -> Any:
    settings = get_settings()
    try:
        import chromadb  # type: ignore
    except ImportError as exc:
        raise RuntimeError("The chromadb package is required for item details.") from exc

    client = chromadb.HttpClient(host=settings.CHROMA_HOST, port=settings.CHROMA_PORT)
    return client.get_or_create_collection(name=collection_name)


def _get_human_description_by_where(collection: Any, where: dict[str, str]) -> str | None:
    result = collection.get(where=where, limit=1, include=["metadatas"])
    return _extract_human_description(result)


def _get_human_description_by_id(collection: Any, entity_id: str) -> str | None:
    result = collection.get(ids=[entity_id], include=["metadatas"])
    return _extract_human_description(result)


def _extract_human_description(result: Any) -> str | None:
    if not isinstance(result, dict):
        return None
    result_dict = cast(dict[str, object], result)

    metadatas_obj = result_dict.get("metadatas")
    if not isinstance(metadatas_obj, list):
        return None
    metadatas = cast(list[object], metadatas_obj)
    if len(metadatas) == 0:
        return None

    first_metadata_obj = metadatas[0]
    if not isinstance(first_metadata_obj, dict):
        return None
    first_metadata = cast(dict[str, object], first_metadata_obj)

    description = first_metadata.get("human_description")
    if isinstance(description, str) and description.strip() != "":
        return description
    return None


def _without_none_values(metadata: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in metadata.items() if value is not None}
