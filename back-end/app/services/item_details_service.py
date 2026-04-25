from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

import duckdb

from app.repositories import item_details_repository
from app.schemas.item_details import ItemDetailsParams, ItemDetailsResponse
from app.services import llm_service


def _ensure_repo_root_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.append(repo_root_str)


_ensure_repo_root_on_path()

from scripts.chroma_seed.models import PersonRecord, TitleRecord  # noqa: E402
from scripts.chroma_seed.prompts import (  # noqa: E402
    build_person_description_prompt,
    build_person_embedding_prompt,
    build_title_description_prompt,
    build_title_embedding_prompt,
)
from app.core.config import get_settings  # noqa: E402


class ItemDetailsNotFoundError(Exception):
    pass


class ItemDetailsUnavailableError(Exception):
    pass


def get_item_details(
    duckdb_conn: duckdb.DuckDBPyConnection,
    params: ItemDetailsParams,
) -> ItemDetailsResponse:
    if params.title_id is not None:
        return _get_title_item_details(duckdb_conn, params.title_id)
    return _get_person_item_details(duckdb_conn, cast(str, params.name_id))


def _get_title_item_details(
    duckdb_conn: duckdb.DuckDBPyConnection,
    title_id: str,
) -> ItemDetailsResponse:
    try:
        cached_description = item_details_repository.get_title_description(title_id)
    except Exception as exc:  # noqa: BLE001
        raise ItemDetailsUnavailableError from exc

    if cached_description is not None:
        return ItemDetailsResponse(
            id=title_id,
            entityType="title",
            description=cached_description,
        )

    prompt_input = item_details_repository.fetch_title_prompt_input(duckdb_conn, title_id)
    if prompt_input is None:
        raise ItemDetailsNotFoundError

    try:
        record = TitleRecord(
            title_id=prompt_input.title_id,
            title=prompt_input.title,
            start_year=prompt_input.start_year,
        )
        settings = get_settings()

        # Generate human description
        human_system, human_user = build_title_description_prompt(record)
        human_description = llm_service.generate_description(
            system_prompt=human_system,
            user_prompt=human_user,
            max_tokens=settings.HUMAN_MAX_TOKENS,
        )

        # Generate embedding description
        embedding_system, embedding_user = build_title_embedding_prompt(record)
        embedding_description = llm_service.generate_description(
            system_prompt=embedding_system,
            user_prompt=embedding_user,
            max_tokens=settings.EMBEDDING_MAX_TOKENS,
        )

        item_details_repository.upsert_title_description(
            title_id=prompt_input.title_id,
            title=prompt_input.title,
            start_year=prompt_input.start_year,
            human_description=human_description,
            embedding_description=embedding_description,
        )
    except Exception as exc:  # noqa: BLE001
        raise ItemDetailsUnavailableError from exc

    return ItemDetailsResponse(
        id=title_id,
        entityType="title",
        description=human_description,
    )


def _get_person_item_details(
    duckdb_conn: duckdb.DuckDBPyConnection,
    name_id: str,
) -> ItemDetailsResponse:
    try:
        cached_description = item_details_repository.get_person_description(name_id)
    except Exception as exc:  # noqa: BLE001
        raise ItemDetailsUnavailableError from exc

    if cached_description is not None:
        return ItemDetailsResponse(
            id=name_id,
            entityType="person",
            description=cached_description,
        )

    prompt_input = item_details_repository.fetch_person_prompt_input(duckdb_conn, name_id)
    if prompt_input is None:
        raise ItemDetailsNotFoundError

    try:
        record = PersonRecord(
            person_id=prompt_input.person_id,
            name=prompt_input.name,
            birth_year=prompt_input.birth_year,
            category=prompt_input.category,
        )
        settings = get_settings()

        # Generate human description
        human_system, human_user = build_person_description_prompt(record)
        human_description = llm_service.generate_description(
            system_prompt=human_system,
            user_prompt=human_user,
            max_tokens=settings.HUMAN_MAX_TOKENS,
        )

        # Generate embedding description
        embedding_system, embedding_user = build_person_embedding_prompt(record)
        embedding_description = llm_service.generate_description(
            system_prompt=embedding_system,
            user_prompt=embedding_user,
            max_tokens=settings.EMBEDDING_MAX_TOKENS,
        )

        item_details_repository.upsert_person_description(
            person_id=prompt_input.person_id,
            name=prompt_input.name,
            birth_year=prompt_input.birth_year,
            category=prompt_input.category,
            human_description=human_description,
            embedding_description=embedding_description,
        )
    except Exception as exc:  # noqa: BLE001
        raise ItemDetailsUnavailableError from exc

    return ItemDetailsResponse(
        id=name_id,
        entityType="person",
        description=human_description,
    )
