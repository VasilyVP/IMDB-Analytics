from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

import duckdb

from app.core.config import get_settings

EntityType = Literal["person", "title"]

_PERSON_LOOKUP_TEMPLATE = """
SELECT DISTINCT nu.nconst AS id, nu.primaryName AS label, nu.birthYear AS birthYear
FROM name_unique nu
WHERE {name_filter}
    AND nu.birthYear IS NOT NULL
{role_filter}
ORDER BY nu.primaryName
LIMIT ?
"""

_TITLE_LOOKUP_TEMPLATE = """
SELECT tb.tconst AS id, tb.primaryTitle AS label, tb.startYear AS startYear
FROM title_basics tb
WHERE tb.primaryTitle ILIKE ?
  AND tb.titleType IN ('movie', 'tvSeries')
    AND tb.startYear IS NOT NULL
ORDER BY tb.titleType ASC, tb.startYear DESC
LIMIT ?
"""


@dataclass(frozen=True, slots=True)
class DuckDBLookupRow:
    id: str
    label: str
    entity_type: EntityType
    birth_year: int | None
    start_year: int | None


@dataclass(frozen=True, slots=True)
class SimilarityRow:
    id: str
    label: str
    entity_type: EntityType
    birth_year: int | None
    start_year: int | None
    score: float | None


def lookup_persons(
    duckdb_conn: duckdb.DuckDBPyConnection,
    name: str,
    limit: int,
    role: Literal["actor", "director"] | None,
) -> list[DuckDBLookupRow]:
    exact_rows = _execute_person_lookup(
        duckdb_conn=duckdb_conn,
        name_filter="lower(nu.primaryName) = lower(?)",
        name_param=name,
        limit=limit,
        role=role,
    )
    if exact_rows:
        return exact_rows

    return _execute_person_lookup(
        duckdb_conn=duckdb_conn,
        name_filter="lower(nu.primaryName) LIKE lower(?)",
        name_param=f"{name}%",
        limit=limit,
        role=role,
    )


def lookup_titles(
    duckdb_conn: duckdb.DuckDBPyConnection,
    title: str,
    limit: int,
) -> list[DuckDBLookupRow]:
    rows = duckdb_conn.execute(_TITLE_LOOKUP_TEMPLATE, [f"%{title}%", limit]).fetchall()
    return _normalize_duckdb_rows(rows, entity_type="title")


def search_similarity(
    query_text: str,
    limit: int,
    entity_type: EntityType,
    category: Literal["actor", "director"] | None,
) -> list[SimilarityRow]:
    settings = get_settings()
    collection_name = (
        settings.CHROMA_COLLECTION_PERSONS
        if entity_type == "person"
        else settings.CHROMA_COLLECTION_TITLES
    )
    collection = _get_collection(collection_name)

    where_clause: dict[str, str] | None = None
    if category is not None:
        where_clause = {"category": category}

    result = collection.query(
        query_texts=[query_text],
        n_results=limit,
        where=where_clause,
        include=["metadatas", "distances"],
    )

    return _normalize_similarity_rows(result, entity_type)


def _execute_person_lookup(
    duckdb_conn: duckdb.DuckDBPyConnection,
    name_filter: str,
    name_param: str,
    limit: int,
    role: Literal["actor", "director"] | None,
) -> list[DuckDBLookupRow]:
    role_filter = ""
    params: list[object] = [name_param]
    if role is not None:
        role_filter = """
AND EXISTS (
    SELECT 1
    FROM title_principals tp
    WHERE tp.nconst = nu.nconst
      AND lower(tp.category) = ?
)
"""
        params.append(role)

    sql = _PERSON_LOOKUP_TEMPLATE.format(name_filter=name_filter, role_filter=role_filter)
    params.append(limit)

    rows = duckdb_conn.execute(sql, params).fetchall()
    return _normalize_duckdb_rows(rows, entity_type="person")



def _normalize_duckdb_rows(
    rows: list[tuple[object, ...]],
    entity_type: EntityType,
) -> list[DuckDBLookupRow]:
    normalized: list[DuckDBLookupRow] = []
    for row in rows:
        entity_id = str(row[0]).strip() if row[0] is not None else ""
        label = str(row[1]).strip() if row[1] is not None else ""
        if entity_id == "" or label == "":
            continue

        birth_year: int | None = None
        start_year: int | None = None
        if entity_type == "person" and len(row) > 2:
            birth_year = _as_optional_int(row[2])
        if entity_type == "title" and len(row) > 2:
            start_year = _as_optional_int(row[2])

        normalized.append(
            DuckDBLookupRow(
                id=entity_id,
                label=label,
                entity_type=entity_type,
                birth_year=birth_year,
                start_year=start_year,
            )
        )
    return normalized


def _normalize_similarity_rows(
    result: object,
    entity_type: EntityType,
) -> list[SimilarityRow]:
    if not isinstance(result, dict):
        return []
    result_dict = cast(dict[str, object], result)

    ids_nested = result_dict.get("ids")
    metadatas_nested = result_dict.get("metadatas")
    distances_nested = result_dict.get("distances")

    if not isinstance(ids_nested, list) or not ids_nested:
        return []
    if not isinstance(metadatas_nested, list) or not metadatas_nested:
        return []

    ids_nested_list = cast(list[object], ids_nested)
    metadatas_nested_list = cast(list[object], metadatas_nested)
    distances_nested_list = cast(list[object], distances_nested) if isinstance(distances_nested, list) else []

    ids = _as_object_list(ids_nested_list[0])
    metadatas = _as_object_list(metadatas_nested_list[0])
    distances = _as_object_list(distances_nested_list[0]) if distances_nested_list else []

    normalized: list[SimilarityRow] = []
    for index, entity_id_obj in enumerate(ids):
        if not isinstance(entity_id_obj, str) or entity_id_obj.strip() == "":
            continue
        metadata_obj = metadatas[index] if index < len(metadatas) else None
        if not isinstance(metadata_obj, dict):
            continue
        metadata = cast(dict[str, object], metadata_obj)

        label_key = "name" if entity_type == "person" else "title"
        label_obj = metadata.get(label_key)
        if not isinstance(label_obj, str) or label_obj.strip() == "":
            continue

        birth_year = _extract_year(metadata, "birth") if entity_type == "person" else None
        start_year = _extract_year(metadata, "start") if entity_type == "title" else None

        score: float | None = None
        distance_obj = distances[index] if index < len(distances) else None
        if isinstance(distance_obj, (int, float)):
            score = max(0.0, 1.0 - float(distance_obj))

        normalized.append(
            SimilarityRow(
                id=entity_id_obj,
                label=label_obj.strip(),
                entity_type=entity_type,
                birth_year=birth_year,
                start_year=start_year,
                score=score,
            )
        )

    normalized.sort(key=lambda row: row.score if row.score is not None else -1.0, reverse=True)
    return normalized


def _as_object_list(value: object) -> list[object]:
    if not isinstance(value, list):
        return []
    return cast(list[object], value)


def _as_optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.isdigit():
            return int(normalized)
    return None


def _extract_year(metadata: dict[str, object], year_type: Literal["birth", "start"]) -> int | None:
    candidate_keys = (
        ["birthYear", "birth_year", "year"]
        if year_type == "birth"
        else ["startYear", "start_year", "year"]
    )
    for key in candidate_keys:
        year = _as_optional_int(metadata.get(key))
        if year is not None:
            return year
    return None


def _get_collection(collection_name: str) -> Any:
    settings = get_settings()
    try:
        import chromadb  # type: ignore
    except ImportError as exc:
        raise RuntimeError("The chromadb package is required for similarity search.") from exc

    client = chromadb.HttpClient(host=settings.CHROMA_HOST, port=settings.CHROMA_PORT)
    return client.get_or_create_collection(name=collection_name)
