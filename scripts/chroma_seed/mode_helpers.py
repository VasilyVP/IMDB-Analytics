from __future__ import annotations

from typing import Callable, TypeAlias, cast

from .chroma_writer import ChromaWriter
from .llm_client import GenerationResult, TextGenerationClient
from .models import (
    ChromaPersonSeedRecord,
    ChromaSeedRecord,
    PersonRecord,
    TitleRecord,
)
from .sqlite_store import SQLiteStore


SourceRecord: TypeAlias = TitleRecord | PersonRecord
SeedRecord: TypeAlias = ChromaSeedRecord | ChromaPersonSeedRecord
FailureRecord: TypeAlias = SourceRecord | SeedRecord


def combine_title_batch_records(
    records: list[SourceRecord],
    human_descriptions: dict[str, str],
    embedding_descriptions: dict[str, str],
) -> list[SeedRecord]:
    title_records = _as_title_records(records)
    combined: list[ChromaSeedRecord] = []
    for title in title_records:
        if title.title_id not in human_descriptions:
            continue
        if title.title_id not in embedding_descriptions:
            continue
        combined.append(
            ChromaSeedRecord(
                title_id=title.title_id,
                title=title.title,
                start_year=title.start_year,
                human_description=human_descriptions[title.title_id],
                embedding_description=embedding_descriptions[title.title_id],
            )
        )
    return list(combined)


def combine_person_batch_records(
    records: list[SourceRecord],
    human_descriptions: dict[str, str],
    embedding_descriptions: dict[str, str],
) -> list[SeedRecord]:
    person_records = _as_person_records(records)
    combined: list[ChromaPersonSeedRecord] = []
    for person in person_records:
        if person.person_id not in human_descriptions:
            continue
        if person.person_id not in embedding_descriptions:
            continue
        combined.append(
            ChromaPersonSeedRecord(
                person_id=person.person_id,
                name=person.name,
                birth_year=person.birth_year,
                category=person.category,
                human_description=human_descriptions[person.person_id],
                embedding_description=embedding_descriptions[person.person_id],
            )
        )
    return list(combined)


def persist_generation_failures(
    store: SQLiteStore,
    records: list[SourceRecord],
    generation_result: GenerationResult,
    phase: str,
    attempt: int,
    get_record_id: Callable[[SourceRecord], str],
    mark_failed_record: Callable[[SQLiteStore, FailureRecord, str, int, str], None],
) -> None:
    by_id = {get_record_id(record): record for record in records}
    for record_id in generation_result.failed_ids:
        record = by_id.get(record_id)
        if record is None:
            continue
        mark_failed_record(
            store,
            record,
            phase,
            attempt,
            generation_result.failure_messages.get(
                record_id,
                f"{phase} request failed after retries",
            ),
        )


def filter_records(
    records: list[SourceRecord],
    descriptions: dict[str, str],
    get_record_id: Callable[[SourceRecord], str],
) -> list[SourceRecord]:
    return [record for record in records if get_record_id(record) in descriptions]


def mark_title_failed_record(
    store: SQLiteStore,
    record: FailureRecord,
    phase: str,
    attempt: int,
    error_message: str,
) -> None:
    if not isinstance(record, (TitleRecord, ChromaSeedRecord)):
        raise TypeError("Expected title record")
    store.mark_title_failed(
        title_id=record.title_id,
        title=record.title,
        start_year=record.start_year,
        phase=phase,
        attempt=attempt,
        error_message=error_message,
    )


def mark_person_failed_record(
    store: SQLiteStore,
    record: FailureRecord,
    phase: str,
    attempt: int,
    error_message: str,
) -> None:
    if not isinstance(record, (PersonRecord, ChromaPersonSeedRecord)):
        raise TypeError("Expected person record")
    store.mark_person_failed(
        person_id=record.person_id,
        name=record.name,
        birth_year=record.birth_year,
        category=record.category,
        phase=phase,
        attempt=attempt,
        error_message=error_message,
    )


def mark_title_success_record(store: SQLiteStore, record: SeedRecord) -> None:
    if not isinstance(record, ChromaSeedRecord):
        raise TypeError("Expected title seed record")
    store.upsert_title_success(
        title_id=record.title_id,
        title=record.title,
        start_year=record.start_year,
        human_description=record.human_description,
        embedding_description=record.embedding_description,
    )


def mark_person_success_record(store: SQLiteStore, record: SeedRecord) -> None:
    if not isinstance(record, ChromaPersonSeedRecord):
        raise TypeError("Expected person seed record")
    store.upsert_person_success(
        person_id=record.person_id,
        name=record.name,
        birth_year=record.birth_year,
        category=record.category,
        human_description=record.human_description,
        embedding_description=record.embedding_description,
    )


def next_consecutive_title_failure_count(
    records: list[SourceRecord],
    failed_ids: set[str],
    previous_value: int,
) -> int:
    title_records = _as_title_records(records)
    consecutive = previous_value
    for title in title_records:
        if title.title_id in failed_ids:
            consecutive += 1
        else:
            consecutive = 0
    return consecutive


def next_consecutive_person_failure_count(
    records: list[SourceRecord],
    failed_ids: set[str],
    previous_value: int,
) -> int:
    person_records = _as_person_records(records)
    consecutive = previous_value
    for person in person_records:
        if person.person_id in failed_ids:
            consecutive += 1
        else:
            consecutive = 0
    return consecutive


def generate_title_human(
    client: TextGenerationClient,
    records: list[SourceRecord],
) -> GenerationResult:
    return client.generate_title_human_descriptions(_as_title_records(records))


def generate_title_embedding(
    client: TextGenerationClient,
    records: list[SourceRecord],
) -> GenerationResult:
    return client.generate_title_embedding_descriptions(_as_title_records(records))


def generate_person_human(
    client: TextGenerationClient,
    records: list[SourceRecord],
) -> GenerationResult:
    return client.generate_person_human_descriptions(_as_person_records(records))


def generate_person_embedding(
    client: TextGenerationClient,
    records: list[SourceRecord],
) -> GenerationResult:
    return client.generate_person_embedding_descriptions(_as_person_records(records))


def upsert_title_batch(writer: ChromaWriter, records: list[SeedRecord]) -> None:
    writer.upsert_title_batch(_as_title_seed_records(records))


def upsert_person_batch(writer: ChromaWriter, records: list[SeedRecord]) -> None:
    writer.upsert_person_batch(_as_person_seed_records(records))


def get_title_record_id(record: SourceRecord) -> str:
    if not isinstance(record, TitleRecord):
        raise TypeError("Expected title record")
    return record.title_id


def get_person_record_id(record: SourceRecord) -> str:
    if not isinstance(record, PersonRecord):
        raise TypeError("Expected person record")
    return record.person_id


def get_title_seed_record_id(record: SeedRecord) -> str:
    if not isinstance(record, ChromaSeedRecord):
        raise TypeError("Expected title seed record")
    return record.title_id


def get_person_seed_record_id(record: SeedRecord) -> str:
    if not isinstance(record, ChromaPersonSeedRecord):
        raise TypeError("Expected person seed record")
    return record.person_id


def _as_title_records(records: list[SourceRecord]) -> list[TitleRecord]:
    if any(not isinstance(record, TitleRecord) for record in records):
        raise TypeError("Expected title records")
    return cast(list[TitleRecord], records)


def _as_person_records(records: list[SourceRecord]) -> list[PersonRecord]:
    if any(not isinstance(record, PersonRecord) for record in records):
        raise TypeError("Expected person records")
    return cast(list[PersonRecord], records)


def _as_title_seed_records(records: list[SeedRecord]) -> list[ChromaSeedRecord]:
    if any(not isinstance(record, ChromaSeedRecord) for record in records):
        raise TypeError("Expected title seed records")
    return cast(list[ChromaSeedRecord], records)


def _as_person_seed_records(records: list[SeedRecord]) -> list[ChromaPersonSeedRecord]:
    if any(not isinstance(record, ChromaPersonSeedRecord) for record in records):
        raise TypeError("Expected person seed records")
    return cast(list[ChromaPersonSeedRecord], records)
