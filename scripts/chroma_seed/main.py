from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

import duckdb

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.chroma_seed.chroma_writer import ChromaWriter
    from scripts.chroma_seed.config import RuntimeConfig, build_parser, load_runtime_config
    from scripts.chroma_seed.duckdb_reader import (
        count_eligible_persons,
        count_eligible_titles,
        fetch_person_batch,
        fetch_title_batch,
    )
    from scripts.chroma_seed.llm_client import GenerationResult, TextGenerationClient
    from scripts.chroma_seed.mode_helpers import (
        FailureRecord,
        SeedRecord,
        SourceRecord,
        combine_person_batch_records,
        combine_title_batch_records,
        filter_records,
        generate_person_embedding,
        generate_person_human,
        generate_title_embedding,
        generate_title_human,
        get_person_record_id,
        get_person_seed_record_id,
        get_title_record_id,
        get_title_seed_record_id,
        mark_person_failed_record,
        mark_person_success_record,
        mark_title_failed_record,
        mark_title_success_record,
        next_consecutive_person_failure_count,
        next_consecutive_title_failure_count,
        persist_generation_failures,
        upsert_person_batch,
        upsert_title_batch,
    )
    from scripts.chroma_seed.models import (
        SummaryCounts,
    )
    from scripts.chroma_seed.progress import (
        ProgressSnapshot,
        create_batch_progress,
        create_overall_progress,
        render_runtime_stats,
    )
    from scripts.chroma_seed.sqlite_store import SQLiteStore
else:
    from .chroma_writer import ChromaWriter
    from .config import RuntimeConfig, build_parser, load_runtime_config
    from .duckdb_reader import (
        count_eligible_persons,
        count_eligible_titles,
        fetch_person_batch,
        fetch_title_batch,
    )
    from .llm_client import GenerationResult, TextGenerationClient
    from .mode_helpers import (
        FailureRecord,
        SeedRecord,
        SourceRecord,
        combine_person_batch_records,
        combine_title_batch_records,
        filter_records,
        generate_person_embedding,
        generate_person_human,
        generate_title_embedding,
        generate_title_human,
        get_person_record_id,
        get_person_seed_record_id,
        get_title_record_id,
        get_title_seed_record_id,
        mark_person_failed_record,
        mark_person_success_record,
        mark_title_failed_record,
        mark_title_success_record,
        next_consecutive_person_failure_count,
        next_consecutive_title_failure_count,
        persist_generation_failures,
        upsert_person_batch,
        upsert_title_batch,
    )
    from .models import SummaryCounts
    from .progress import (
        ProgressSnapshot,
        create_batch_progress,
        create_overall_progress,
        render_runtime_stats,
    )
    from .sqlite_store import SQLiteStore


@dataclass(frozen=True, slots=True)
class _ModeRunnerConfig:
    mode_name: str
    collection_name: str
    noun: str
    count_available: Callable[[duckdb.DuckDBPyConnection, str | None], int]
    fetch_batch: Callable[[duckdb.DuckDBPyConnection, int, str | None], Sequence[SourceRecord]]
    get_last_success_id: Callable[[SQLiteStore], str | None]
    generate_human: Callable[[TextGenerationClient, list[SourceRecord]], GenerationResult]
    generate_embedding: Callable[[TextGenerationClient, list[SourceRecord]], GenerationResult]
    combine_records: Callable[[list[SourceRecord], dict[str, str], dict[str, str]], list[SeedRecord]]
    get_record_id: Callable[[SourceRecord], str]
    get_seed_record_id: Callable[[SeedRecord], str]
    mark_failed_record: Callable[[SQLiteStore, FailureRecord, str, int, str], None]
    mark_success_record: Callable[[SQLiteStore, SeedRecord], None]
    upsert_batch: Callable[[ChromaWriter, list[SeedRecord]], None]
    get_summary_counts: Callable[[SQLiteStore], SummaryCounts]
    next_consecutive_failure_count: Callable[[list[SourceRecord], set[str], int], int]


def main() -> None:
    args = build_parser().parse_args()
    config = load_runtime_config(
        batch_size=args.batch_size,
        limit=args.limit,
        run_titles=args.titles,
        run_persons=args.persons,
    )

    try:
        duckdb_connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    except duckdb.Error as exc:
        print(f"Failed to open DuckDB at {config.duckdb_path}: {exc}", flush=True)
        raise SystemExit(1) from exc

    store = SQLiteStore(config.sqlite_path)
    store.initialize_schema()

    reset_requested = _should_reset_existing_state(store, config.selected_modes)
    if reset_requested:
        if "titles" in config.selected_modes:
            store.clear_titles()
        if "persons" in config.selected_modes:
            store.clear_persons()

    generation_client = TextGenerationClient(
        model=config.model,
        base_url=config.openai_base_url,
        api_key=config.openai_api_key,
        max_retries=config.max_retries,
        human_max_tokens=config.human_max_tokens,
        embedding_max_tokens=config.embedding_max_tokens,
        inference_concurrency=config.inference_concurrency,
    )

    try:
        stop_reason: str | None = None
        consecutive_failed_records = 0
        for mode in config.selected_modes:
            mode_config = _build_mode_runner_config(config, mode)
            stop_reason, consecutive_failed_records = _run_mode(
                config=config,
                store=store,
                generation_client=generation_client,
                duckdb_connection=duckdb_connection,
                reset_requested=reset_requested,
                previous_consecutive_failures=consecutive_failed_records,
                mode_config=mode_config,
            )

            if stop_reason is not None:
                print(stop_reason, flush=True)
                raise SystemExit(1)
    finally:
        duckdb_connection.close()


def _should_reset_existing_state(store: SQLiteStore, selected_modes: tuple[str, ...]) -> bool:
    has_state = ("titles" in selected_modes and store.has_title_records()) or (
        "persons" in selected_modes and store.has_person_records()
    )
    if not has_state:
        return False

    if not sys.stdin.isatty():
        return False

    while True:
        choice = input(
            "Existing SQLite state found. Continue from last success or restart? [c/r]: "
        ).strip().lower()
        if choice in {"c", "continue", ""}:
            return False
        if choice in {"r", "restart"}:
            return True
        print("Please answer with 'c' to continue or 'r' to restart.", flush=True)


def _run_mode(
    config: RuntimeConfig,
    store: SQLiteStore,
    generation_client: TextGenerationClient,
    duckdb_connection: duckdb.DuckDBPyConnection,
    reset_requested: bool,
    previous_consecutive_failures: int,
    mode_config: _ModeRunnerConfig,
) -> tuple[str | None, int]:
    writer = ChromaWriter(
        collection_name=mode_config.collection_name,
        max_retries=config.max_retries,
        host=config.chroma_host,
        port=config.chroma_port,
    )

    resume_id = None if reset_requested else mode_config.get_last_success_id(store)
    duckdb_query_seconds = 0.0
    total_target = 0
    start_time = time.perf_counter()
    processed = 0
    success = 0
    failed = 0
    generation_seconds = 0.0
    chromadb_save_seconds = 0.0
    stop_reason: str | None = None
    consecutive_failed_records = previous_consecutive_failures

    overall_bar = None
    batch_bar = None
    try:
        writer.ensure_collection(reset=reset_requested)

        duckdb_count_start = time.perf_counter()
        total_available = mode_config.count_available(duckdb_connection, resume_id)
        duckdb_query_seconds += time.perf_counter() - duckdb_count_start
        total_target = min(total_available, config.limit) if config.limit else total_available
        print(
            f"Seeding collection '{mode_config.collection_name}' with {total_target} {mode_config.noun}.",
            flush=True,
        )

        overall_bar = create_overall_progress(total=total_target)
        batch_bar = create_batch_progress()

        while processed < total_target:
            remaining = total_target - processed
            current_batch_size = min(config.batch_size, remaining)
            duckdb_batch_start = time.perf_counter()
            records = list(
                mode_config.fetch_batch(
                    duckdb_connection,
                    current_batch_size,
                    resume_id,
                )
            )
            duckdb_query_seconds += time.perf_counter() - duckdb_batch_start
            if not records:
                break

            batch_bar.reset(total=len(records))
            batch_bar.set_description("Batch")
            batch_failed_ids: set[str] = set()

            try:
                generation_start = time.perf_counter()
                human_result = mode_config.generate_human(generation_client, records)
                persist_generation_failures(
                    store=store,
                    records=records,
                    generation_result=human_result,
                    phase="human_generation",
                    attempt=config.max_retries,
                    get_record_id=mode_config.get_record_id,
                    mark_failed_record=mode_config.mark_failed_record,
                )

                records_with_human = filter_records(
                    records,
                    human_result.descriptions,
                    mode_config.get_record_id,
                )
                embedding_result = mode_config.generate_embedding(generation_client, records_with_human)
                persist_generation_failures(
                    store=store,
                    records=records_with_human,
                    generation_result=embedding_result,
                    phase="embedding_generation",
                    attempt=config.max_retries,
                    get_record_id=mode_config.get_record_id,
                    mark_failed_record=mode_config.mark_failed_record,
                )
                generation_seconds += time.perf_counter() - generation_start

                seed_records = mode_config.combine_records(
                    records,
                    human_result.descriptions,
                    embedding_result.descriptions,
                )

                record_id_by_source_record = {
                    mode_config.get_record_id(record): record for record in records
                }
                batch_failed_ids.update(human_result.failed_ids)
                batch_failed_ids.update(embedding_result.failed_ids)

                seed_record_ids = {
                    mode_config.get_seed_record_id(seed_record) for seed_record in seed_records
                }
                dropped_record_ids = {
                    record_id
                    for record_id in record_id_by_source_record
                    if record_id not in seed_record_ids and record_id not in batch_failed_ids
                }
                for dropped_record_id in dropped_record_ids:
                    source_record = record_id_by_source_record[dropped_record_id]
                    mode_config.mark_failed_record(
                        store,
                        source_record,
                        "record_combination",
                        config.max_retries,
                        "Record could not be prepared for Chroma write.",
                    )
                batch_failed_ids.update(dropped_record_ids)

                write_failed_ids: set[str] = set()
                if seed_records:
                    chromadb_save_start = time.perf_counter()
                    try:
                        mode_config.upsert_batch(writer, seed_records)
                    except Exception as exc:  # noqa: BLE001
                        for seed_record in seed_records:
                            write_failed_ids.add(mode_config.get_seed_record_id(seed_record))
                            mode_config.mark_failed_record(
                                store,
                                seed_record,
                                "chroma_write",
                                config.max_retries,
                                str(exc),
                            )
                    else:
                        for seed_record in seed_records:
                            mode_config.mark_success_record(store, seed_record)
                    finally:
                        chromadb_save_seconds += time.perf_counter() - chromadb_save_start
                batch_failed_ids.update(write_failed_ids)
            except Exception as exc:  # noqa: BLE001
                for record in records:
                    record_id = mode_config.get_record_id(record)
                    if record_id in batch_failed_ids:
                        continue
                    mode_config.mark_failed_record(
                        store,
                        record,
                        "mode_round",
                        config.max_retries,
                        str(exc),
                    )
                    batch_failed_ids.add(record_id)
                batch_success_count = 0
                batch_failed_count = len(batch_failed_ids)
                processed += len(records)
                failed += batch_failed_count
                overall_bar.update(len(records))
                batch_bar.update(len(records))
                resume_id = mode_config.get_record_id(records[-1])
                consecutive_failed_records = mode_config.next_consecutive_failure_count(
                    records,
                    batch_failed_ids,
                    consecutive_failed_records,
                )
                stop_reason = f"Mode '{mode_config.mode_name}' failed: {exc}"
                break

            batch_success_count = len(records) - len(batch_failed_ids)
            batch_failed_count = len(batch_failed_ids)

            processed += len(records)
            success += batch_success_count
            failed += batch_failed_count
            overall_bar.update(len(records))
            batch_bar.update(len(records))
            resume_id = mode_config.get_record_id(records[-1])

            consecutive_failed_records = mode_config.next_consecutive_failure_count(
                records,
                batch_failed_ids,
                consecutive_failed_records,
            )
            if consecutive_failed_records >= config.max_consecutive_failures:
                stop_reason = (
                    "Stopped due to consecutive record failure threshold "
                    f"({config.max_consecutive_failures})."
                )
                break
    except Exception as exc:  # noqa: BLE001
        failed = max(failed, 1)
        stop_reason = f"Mode '{mode_config.mode_name}' failed: {exc}"
    finally:
        if overall_bar is not None:
            overall_bar.close()
        if batch_bar is not None:
            batch_bar.close()

    elapsed = time.perf_counter() - start_time
    snapshot = ProgressSnapshot(
        processed=processed,
        total=total_target,
        success=success,
        failed=failed,
        elapsed_seconds=elapsed,
        generation_seconds=generation_seconds,
        chromadb_save_seconds=chromadb_save_seconds,
        duckdb_query_seconds=duckdb_query_seconds,
    )
    print(render_runtime_stats(snapshot), flush=True)
    print(
        (
            f"final_state[{mode_config.mode_name}]="
            f"processed:{processed}/{total_target} "
            f"success:{success} "
            f"failed:{failed}"
        ),
        flush=True,
    )

    return stop_reason, consecutive_failed_records


def _build_mode_runner_config(config: RuntimeConfig, mode: str) -> _ModeRunnerConfig:
    if mode == "titles":
        return _ModeRunnerConfig(
            mode_name="titles",
            collection_name=config.collection_name_titles,
            noun="titles",
            count_available=count_eligible_titles,
            fetch_batch=fetch_title_batch,
            get_last_success_id=lambda store: store.get_last_success_title_id(),
            generate_human=generate_title_human,
            generate_embedding=generate_title_embedding,
            combine_records=combine_title_batch_records,
            get_record_id=get_title_record_id,
            get_seed_record_id=get_title_seed_record_id,
            mark_failed_record=mark_title_failed_record,
            mark_success_record=mark_title_success_record,
            upsert_batch=upsert_title_batch,
            get_summary_counts=lambda store: store.get_summary_counts(),
            next_consecutive_failure_count=next_consecutive_title_failure_count,
        )

    return _ModeRunnerConfig(
        mode_name="persons",
        collection_name=config.collection_name_persons,
        noun="persons",
        count_available=count_eligible_persons,
        fetch_batch=fetch_person_batch,
        get_last_success_id=lambda store: store.get_last_success_person_id(),
        generate_human=generate_person_human,
        generate_embedding=generate_person_embedding,
        combine_records=combine_person_batch_records,
        get_record_id=get_person_record_id,
        get_seed_record_id=get_person_seed_record_id,
        mark_failed_record=mark_person_failed_record,
        mark_success_record=mark_person_success_record,
        upsert_batch=upsert_person_batch,
        get_summary_counts=lambda store: store.get_person_summary_counts(),
        next_consecutive_failure_count=next_consecutive_person_failure_count,
    )


if __name__ == "__main__":
    main()
