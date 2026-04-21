from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.chroma_seed.chroma_writer import ChromaWriter
    from scripts.chroma_seed.config import build_parser, load_runtime_config
    from scripts.chroma_seed.duckdb_reader import count_eligible_titles, fetch_title_batch
    from scripts.chroma_seed.llm_client import GenerationResult, TextGenerationClient
    from scripts.chroma_seed.models import ChromaSeedRecord, TitleRecord
    from scripts.chroma_seed.progress import (
        ProgressSnapshot,
        create_batch_progress,
        create_overall_progress,
        render_runtime_stats,
    )
    from scripts.chroma_seed.sqlite_store import SQLiteStore
else:
    from .chroma_writer import ChromaWriter
    from .config import build_parser, load_runtime_config
    from .duckdb_reader import count_eligible_titles, fetch_title_batch
    from .llm_client import GenerationResult, TextGenerationClient
    from .models import ChromaSeedRecord, TitleRecord
    from .progress import (
        ProgressSnapshot,
        create_batch_progress,
        create_overall_progress,
        render_runtime_stats,
    )
    from .sqlite_store import SQLiteStore


def main() -> None:
    args = build_parser().parse_args()
    config = load_runtime_config(batch_size=args.batch_size, limit=args.limit)

    try:
        duckdb_connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    except duckdb.Error as exc:
        print(f"Failed to open DuckDB at {config.duckdb_path}: {exc}", flush=True)
        raise SystemExit(1) from exc

    store = SQLiteStore(config.sqlite_path)
    store.initialize_schema()

    reset_requested = _should_reset_existing_state(store)
    if reset_requested:
        store.clear_all()

    resume_title_id = None if reset_requested else store.get_last_success_title_id()

    try:
        writer = ChromaWriter(
            collection_name=config.collection_name,
            max_retries=config.max_retries,
            host=config.chroma_host,
            port=config.chroma_port,
        )
        writer.ensure_collection(reset=reset_requested)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to initialize ChromaDB: {exc}", flush=True)
        raise SystemExit(1) from exc

    generation_client = TextGenerationClient(
        model=config.model,
        base_url=config.openai_base_url,
        api_key=config.openai_api_key,
        max_retries=config.max_retries,
        human_max_tokens=config.human_max_tokens,
        embedding_max_tokens=config.embedding_max_tokens,
        inference_concurrency=config.inference_concurrency,
    )

    duckdb_query_seconds = 0.0
    duckdb_count_start = time.perf_counter()
    total_available = count_eligible_titles(
        duckdb_connection,
        after_title_id=resume_title_id,
    )
    duckdb_query_seconds += time.perf_counter() - duckdb_count_start
    total_target = min(total_available, config.limit) if config.limit else total_available

    print(
        f"Seeding collection '{config.collection_name}' with {total_target} titles.",
        flush=True,
    )

    start_time = time.perf_counter()
    processed = 0
    success = 0
    failed = 0
    generation_seconds = 0.0
    chromadb_save_seconds = 0.0
    consecutive_failed_titles = 0
    stop_reason: str | None = None

    overall_bar = create_overall_progress(total=total_target)
    batch_bar = create_batch_progress()

    try:
        while processed < total_target:
            remaining = total_target - processed
            current_batch_size = min(config.batch_size, remaining)
            duckdb_batch_start = time.perf_counter()
            titles = fetch_title_batch(
                duckdb_connection,
                batch_size=current_batch_size,
                after_title_id=resume_title_id,
            )
            duckdb_query_seconds += time.perf_counter() - duckdb_batch_start
            if not titles:
                break

            batch_bar.reset(total=len(titles))
            batch_bar.set_description("Batch")

            generation_start = time.perf_counter()
            human_result = generation_client.generate_human_descriptions(titles)
            _persist_generation_failures(
                store=store,
                titles=titles,
                generation_result=human_result,
                phase="human_generation",
                attempt=config.max_retries,
            )

            titles_with_human = _filter_titles(titles, human_result.descriptions)
            embedding_result = generation_client.generate_embedding_descriptions(titles_with_human)
            _persist_generation_failures(
                store=store,
                titles=titles_with_human,
                generation_result=embedding_result,
                phase="embedding_generation",
                attempt=config.max_retries,
            )
            generation_seconds += time.perf_counter() - generation_start

            records = _combine_batch_records(
                titles=titles,
                human_descriptions=human_result.descriptions,
                embedding_descriptions=embedding_result.descriptions,
            )

            write_failed_title_ids: set[str] = set()
            if records:
                chromadb_save_start = time.perf_counter()
                try:
                    writer.upsert_batch(records)
                except Exception as exc:  # noqa: BLE001
                    for record in records:
                        write_failed_title_ids.add(record.title_id)
                        store.mark_failed(
                            title_id=record.title_id,
                            title=record.title,
                            start_year=record.start_year,
                            phase="chroma_write",
                            attempt=config.max_retries,
                            error_message=str(exc),
                        )
                else:
                    for record in records:
                        store.upsert_success(
                            title_id=record.title_id,
                            title=record.title,
                            start_year=record.start_year,
                            human_description=record.human_description,
                            embedding_description=record.embedding_description,
                        )
                finally:
                    chromadb_save_seconds += time.perf_counter() - chromadb_save_start

            batch_failed_ids = set(human_result.failed_title_ids)
            batch_failed_ids.update(embedding_result.failed_title_ids)
            batch_failed_ids.update(write_failed_title_ids)

            batch_success_count = len(titles) - len(batch_failed_ids)
            batch_failed_count = len(batch_failed_ids)

            processed += len(titles)
            success += batch_success_count
            failed += batch_failed_count
            overall_bar.update(len(titles))
            batch_bar.update(len(titles))
            resume_title_id = titles[-1].title_id

            consecutive_failed_titles = _next_consecutive_failure_count(
                titles=titles,
                failed_title_ids=batch_failed_ids,
                previous_value=consecutive_failed_titles,
            )
            if consecutive_failed_titles >= config.max_consecutive_title_failures:
                stop_reason = (
                    "Stopped due to consecutive title failure threshold "
                    f"({config.max_consecutive_title_failures})."
                )
                break
    finally:
        overall_bar.close()
        batch_bar.close()
        duckdb_connection.close()

    elapsed = time.perf_counter() - start_time
    summary_counts = store.get_summary_counts()
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
            "final_state="
            f"processed:{processed}/{total_target} "
            f"success:{summary_counts.success_count} "
            f"failed:{summary_counts.failed_count}"
        ),
        flush=True,
    )

    if stop_reason is not None:
        print(stop_reason, flush=True)
        raise SystemExit(1)


def _should_reset_existing_state(store: SQLiteStore) -> bool:
    if not store.has_records():
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


def _combine_batch_records(
    titles: list[TitleRecord],
    human_descriptions: dict[str, str],
    embedding_descriptions: dict[str, str],
) -> list[ChromaSeedRecord]:
    records: list[ChromaSeedRecord] = []
    for title in titles:
        if title.title_id not in human_descriptions:
            continue
        if title.title_id not in embedding_descriptions:
            continue
        records.append(
            ChromaSeedRecord(
                title_id=title.title_id,
                title=title.title,
                start_year=title.start_year,
                human_description=human_descriptions[title.title_id],
                embedding_description=embedding_descriptions[title.title_id],
            )
        )
    return records


def _persist_generation_failures(
    store: SQLiteStore,
    titles: list[TitleRecord],
    generation_result: GenerationResult,
    phase: str,
    attempt: int,
 ) -> None:
    by_id = {title.title_id: title for title in titles}
    for title_id in generation_result.failed_title_ids:
        title = by_id.get(title_id)
        if title is None:
            continue
        store.mark_failed(
            title_id=title_id,
            title=title.title,
            start_year=title.start_year,
            phase=phase,
            attempt=attempt,
            error_message=generation_result.failure_messages.get(
                title_id,
                f"{phase} request failed after retries",
            ),
        )


def _filter_titles(
    titles: list[TitleRecord],
    descriptions: dict[str, str],
) -> list[TitleRecord]:
    return [title for title in titles if title.title_id in descriptions]


def _next_consecutive_failure_count(
    titles: list[TitleRecord],
    failed_title_ids: set[str],
    previous_value: int,
) -> int:
    consecutive = previous_value
    for title in titles:
        if title.title_id in failed_title_ids:
            consecutive += 1
        else:
            consecutive = 0
    return consecutive


if __name__ == "__main__":
    main()
