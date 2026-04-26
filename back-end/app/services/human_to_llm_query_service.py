from __future__ import annotations

import json
from pathlib import Path

import duckdb

from app.repositories import human_to_llm_repository
from app.schemas.human_to_llm_query import (
    ClassifiedQuery,
    HumanToLlmQueryRequest,
    HumanToLlmQueryResponse,
    HumanToLlmResultItem,
)
from app.services import llm_service

_PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "human_to_llm_query_system.txt"


class HumanToLlmQueryParseError(Exception):
    pass


class HumanToLlmQueryUnavailableError(Exception):
    pass


def human_to_llm_query(
    duckdb_conn: duckdb.DuckDBPyConnection,
    params: HumanToLlmQueryRequest,
) -> HumanToLlmQueryResponse:
    classified = _classify_query(params.query)

    try:
        if classified.type == "person":
            if classified.parsed.name is None:
                raise HumanToLlmQueryParseError("person classification requires parsed.name")
            rows = human_to_llm_repository.lookup_persons(
                duckdb_conn,
                name=classified.parsed.name,
                limit=params.limit,
                role=classified.parsed.role,
            )
            return _build_lookup_response(classified, rows)

        if classified.type == "film":
            if classified.parsed.title is None:
                raise HumanToLlmQueryParseError("film classification requires parsed.title")
            rows = human_to_llm_repository.lookup_titles(
                duckdb_conn,
                title=classified.parsed.title,
                limit=params.limit,
            )
            return _build_lookup_response(classified, rows)

        if (
            classified.type == "person_search"
            and classified.parsed.name is not None
            and classified.parsed.role is not None
        ):
            rows = human_to_llm_repository.lookup_persons(
                duckdb_conn,
                name=classified.parsed.name,
                limit=params.limit,
                role=classified.parsed.role,
            )
            return _build_lookup_response(classified, rows)

        details_query = classified.parsed.details
        if classified.type == "recommendation" and details_query is None:
            details_query = params.query
        if details_query is None:
            raise HumanToLlmQueryParseError("similarity classification requires parsed.details")

        similarity_entity_type = _resolve_similarity_entity_type(classified)
        rows = human_to_llm_repository.search_similarity(
            query_text=details_query,
            limit=params.limit,
            entity_type=similarity_entity_type,
            category=classified.parsed.role,
        )
        return _build_similarity_response(classified, rows)
    except HumanToLlmQueryParseError:
        raise
    except duckdb.Error as exc:
        raise HumanToLlmQueryUnavailableError from exc
    except RuntimeError as exc:
        raise HumanToLlmQueryUnavailableError from exc


def _classify_query(query: str) -> ClassifiedQuery:
    prompt = _load_system_prompt()
    try:
        completion = llm_service.request_completion(
            system_prompt=prompt,
            user_prompt=query,
            max_tokens=300,
        )

        payload = _extract_json_payload(completion)
        
        return ClassifiedQuery.model_validate(payload)
    except Exception as exc:  # noqa: BLE001
        raise HumanToLlmQueryParseError from exc


def _extract_json_payload(content: str) -> object:
    normalized = content.strip()
    if normalized.startswith("```"):
        normalized = normalized.strip("`")
        if normalized.lower().startswith("json"):
            normalized = normalized[4:].strip()

    start = normalized.find("{")
    end = normalized.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("LLM output does not contain a JSON object")

    return json.loads(normalized[start : end + 1])


def _resolve_similarity_entity_type(classified: ClassifiedQuery) -> human_to_llm_repository.EntityType:
    if classified.type == "person_search":
        return "person"
    if classified.type == "film_search":
        return "title"
    if classified.parsed.role is not None:
        return "person"
    if classified.parsed.name is not None and classified.parsed.title is None:
        return "person"
    return "title"


def _build_lookup_response(
    classified: ClassifiedQuery,
    rows: list[human_to_llm_repository.DuckDBLookupRow],
) -> HumanToLlmQueryResponse:
    return HumanToLlmQueryResponse(
        type=classified.type,
        parsed=classified.parsed,
        results=[
            HumanToLlmResultItem(
                id=row.id,
                label=row.label,
                entityType=row.entity_type,
                birthYear=row.birth_year,
                startYear=row.start_year,
                score=None,
            )
            for row in rows
        ],
    )


def _build_similarity_response(
    classified: ClassifiedQuery,
    rows: list[human_to_llm_repository.SimilarityRow],
) -> HumanToLlmQueryResponse:
    return HumanToLlmQueryResponse(
        type=classified.type,
        parsed=classified.parsed,
        results=[
            HumanToLlmResultItem(
                id=row.id,
                label=row.label,
                entityType=row.entity_type,
                birthYear=row.birth_year,
                startYear=row.start_year,
                score=row.score,
            )
            for row in rows
        ],
    )


def _load_system_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")
