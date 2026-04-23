from __future__ import annotations

import sqlite3
import json
import io
import types
import tempfile
import unittest
import importlib
from contextlib import closing, redirect_stdout
from pathlib import Path
from typing import Any, Callable, cast
from unittest.mock import patch

import duckdb

_REPO_ROOT = Path(__file__).resolve().parents[2]
import sys

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.chroma_seed.duckdb_reader import (  # noqa: E402
    count_eligible_titles,
    count_eligible_persons,
    fetch_title_batch,
    fetch_person_batch,
)
import scripts.chroma_seed.config as chroma_config  # noqa: E402
import scripts.chroma_seed.main as chroma_main  # noqa: E402
from scripts.chroma_seed.chroma_writer import ChromaWriter  # noqa: E402
from scripts.chroma_seed.llm_client import GenerationResult, TextGenerationClient  # noqa: E402
import scripts.chroma_seed.prompts as chroma_prompts  # noqa: E402
from scripts.chroma_seed.models import ChromaPersonSeedRecord, PersonRecord, TitleRecord  # noqa: E402
from scripts.chroma_seed.config import load_runtime_config  # noqa: E402
from scripts.chroma_seed.progress import ProgressSnapshot, render_runtime_stats  # noqa: E402
from scripts.chroma_seed.sqlite_store import SQLiteStore  # noqa: E402


class SQLiteStoreTests(unittest.TestCase):
    def test_initialize_creates_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            with closing(sqlite3.connect(sqlite_path)) as connection:
                rows = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()

            table_names = {row[0] for row in rows}
            self.assertIn("seed_titles", table_names)
            self.assertIn("seed_persons", table_names)
            self.assertIn("seed_failures", table_names)

    def test_last_successful_person_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            store.upsert_person_success(
                person_id="nm0000002",
                name="Second",
                birth_year=1970,
                category="actor",
                human_description="Human",
                embedding_description="Embedding",
            )
            store.upsert_person_success(
                person_id="nm0000010",
                name="Tenth",
                birth_year=1960,
                category="director",
                human_description="Human",
                embedding_description="Embedding",
            )

            self.assertEqual(store.get_last_success_person_id(), "nm0000010")

    def test_last_successful_title_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            store.upsert_success(
                title_id="tt0000002",
                title="Second",
                start_year=2001,
                human_description="Human",
                embedding_description="Embedding",
            )
            store.upsert_success(
                title_id="tt0000010",
                title="Tenth",
                start_year=2002,
                human_description="Human",
                embedding_description="Embedding",
            )

            self.assertEqual(store.get_last_success_title_id(), "tt0000010")

    def test_failed_title_is_persisted_with_failure_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            store.mark_failed(
                title_id="tt1234567",
                title="Failure Case",
                start_year=1999,
                phase="human_generation",
                attempt=3,
                error_message="generation failed",
            )

            summary = store.get_summary_counts()
            self.assertEqual(summary.success_count, 0)
            self.assertEqual(summary.failed_count, 1)

            with closing(sqlite3.connect(sqlite_path)) as connection:
                failure_rows = connection.execute(
                    "SELECT record_id, phase, attempt, error_message FROM seed_failures"
                ).fetchall()

            self.assertEqual(len(failure_rows), 1)
            self.assertEqual(
                failure_rows[0],
                ("tt1234567", "human_generation", 3, "generation failed"),
            )

    def test_failed_person_is_persisted_with_failure_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            store.mark_person_failed(
                person_id="nm1234567",
                name="Failure Person",
                birth_year=1975,
                category="actor",
                phase="embedding_generation",
                attempt=3,
                error_message="generation failed",
            )

            summary = store.get_person_summary_counts()
            self.assertEqual(summary.success_count, 0)
            self.assertEqual(summary.failed_count, 1)

            with closing(sqlite3.connect(sqlite_path)) as connection:
                failure_rows = connection.execute(
                    "SELECT record_id, phase, attempt, error_message FROM seed_failures"
                ).fetchall()

            self.assertEqual(len(failure_rows), 1)
            self.assertEqual(
                failure_rows[0],
                ("nm1234567", "embedding_generation", 3, "generation failed"),
            )


class DuckDBReaderTests(unittest.TestCase):
    def test_count_and_fetch_apply_spec_filters_and_ordering(self) -> None:
        connection = duckdb.connect(database=":memory:")
        connection.execute(
            """
            CREATE TABLE title_basics (
                tconst TEXT,
                titleType TEXT,
                primaryTitle TEXT,
                startYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_ratings (
                tconst TEXT,
                averageRating DOUBLE,
                numVotes INTEGER
            );
            """
        )

        connection.execute(
            """
            CREATE TABLE name_unique (
                nconst TEXT,
                primaryName TEXT,
                birthYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT
            );
            """
        )

        connection.execute(
            """
            INSERT INTO title_basics VALUES
                ('tt0000001', 'movie', 'A Movie', 2015),
                ('tt0000002', 'movie', 'B Movie', 2016),
                ('tt0000003', 'tvSeries', 'Series', 2017),
                ('tt0000004', 'movie', 'Old Movie', 2010),
                ('tt0000005', 'movie', 'Recent Movie', 2024),
                ('tt0000006', 'movie', 'Low Rated', 2018);
            """
        )
        connection.execute(
            """
            INSERT INTO title_ratings VALUES
                ('tt0000001', 8.0, 1000),
                ('tt0000002', 7.6, 1000),
                ('tt0000003', 9.0, 1000),
                ('tt0000004', 8.5, 1000),
                ('tt0000005', 8.7, 1000),
                ('tt0000006', 7.4, 1000);
            """
        )

        connection.execute(
            """
            INSERT INTO name_unique VALUES
                ('nm0000001', 'Actor One', 1980),
                ('nm0000002', 'Director Two', 1975),
                ('nm0000003', 'Writer Three', 1971);
            """
        )
        connection.execute(
            """
            INSERT INTO title_principals VALUES
                ('tt0000001', 1, 'nm0000001', 'actor', NULL, NULL),
                ('tt0000002', 1, 'nm0000002', 'director', NULL, NULL),
                ('tt0000002', 2, 'nm0000003', 'writer', NULL, NULL);
            """
        )

        self.assertEqual(count_eligible_titles(connection), 2)

        rows = fetch_title_batch(connection, batch_size=10, after_title_id=None)
        self.assertEqual([row.title_id for row in rows], ["tt0000001", "tt0000002"])

        resumed_rows = fetch_title_batch(
            connection,
            batch_size=10,
            after_title_id="tt0000001",
        )
        self.assertEqual([row.title_id for row in resumed_rows], ["tt0000002"])

        self.assertEqual(count_eligible_persons(connection), 2)
        people = fetch_person_batch(connection, batch_size=10, after_person_id=None)
        self.assertEqual([row.person_id for row in people], ["nm0000001", "nm0000002"])

    def test_person_queries_count_and_fetch_unique_person_ids(self) -> None:
        connection = duckdb.connect(database=":memory:")
        connection.execute(
            """
            CREATE TABLE title_basics (
                tconst TEXT,
                titleType TEXT,
                primaryTitle TEXT,
                startYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_ratings (
                tconst TEXT,
                averageRating DOUBLE,
                numVotes INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE name_unique (
                nconst TEXT,
                primaryName TEXT,
                birthYear INTEGER
            );
            """
        )

        connection.execute(
            """
            INSERT INTO title_basics VALUES
                ('tt0000001', 'movie', 'A Movie', 2018),
                ('tt0000002', 'movie', 'B Movie', 2019);
            """
        )
        connection.execute(
            """
            INSERT INTO title_ratings VALUES
                ('tt0000001', 8.0, 1000),
                ('tt0000002', 7.6, 1000);
            """
        )
        connection.execute(
            """
            INSERT INTO name_unique VALUES
                ('nm0000001', 'Actor One', 1980);
            """
        )
        connection.execute(
            """
            INSERT INTO title_principals VALUES
                ('tt0000001', 1, 'nm0000001', 'actor', NULL, NULL),
                ('tt0000002', 1, 'nm0000001', 'actor', NULL, NULL);
            """
        )

        self.assertEqual(count_eligible_persons(connection), 1)
        people = fetch_person_batch(connection, batch_size=10, after_person_id=None)
        self.assertEqual([row.person_id for row in people], ["nm0000001"])

    def test_person_resume_fetches_only_after_last_success_id(self) -> None:
        connection = duckdb.connect(database=":memory:")
        connection.execute(
            """
            CREATE TABLE title_basics (
                tconst TEXT,
                titleType TEXT,
                primaryTitle TEXT,
                startYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_ratings (
                tconst TEXT,
                averageRating DOUBLE,
                numVotes INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE name_unique (
                nconst TEXT,
                primaryName TEXT,
                birthYear INTEGER
            );
            """
        )

        connection.executemany(
            "INSERT INTO title_basics VALUES (?, ?, ?, ?)",
            [
                ("tt0000001", "movie", "Movie 1", 2018),
                ("tt0000002", "movie", "Movie 2", 2018),
                ("tt0000003", "movie", "Movie 3", 2018),
            ],
        )
        connection.executemany(
            "INSERT INTO title_ratings VALUES (?, ?, ?)",
            [
                ("tt0000001", 8.1, 1000),
                ("tt0000002", 8.1, 1000),
                ("tt0000003", 8.1, 1000),
            ],
        )
        connection.executemany(
            "INSERT INTO name_unique VALUES (?, ?, ?)",
            [
                ("nm0000001", "Person 1", 1980),
                ("nm0000002", "Person 2", 1980),
                ("nm0000003", "Person 3", 1980),
            ],
        )
        connection.executemany(
            "INSERT INTO title_principals VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("tt0000001", 1, "nm0000001", "actor", None, None),
                ("tt0000002", 1, "nm0000002", "actor", None, None),
                ("tt0000003", 1, "nm0000003", "actor", None, None),
            ],
        )

        resumed = fetch_person_batch(connection, batch_size=10, after_person_id="nm0000001")
        self.assertEqual([row.person_id for row in resumed], ["nm0000002", "nm0000003"])

    def test_person_categories_are_aggregated_distinctly(self) -> None:
        connection = duckdb.connect(database=":memory:")
        connection.execute(
            """
            CREATE TABLE title_basics (
                tconst TEXT,
                titleType TEXT,
                primaryTitle TEXT,
                startYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_ratings (
                tconst TEXT,
                averageRating DOUBLE,
                numVotes INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE name_unique (
                nconst TEXT,
                primaryName TEXT,
                birthYear INTEGER
            );
            """
        )

        connection.executemany(
            "INSERT INTO title_basics VALUES (?, ?, ?, ?)",
            [
                ("tt0000001", "movie", "Movie A", 2018),
                ("tt0000002", "movie", "Movie B", 2019),
            ],
        )
        connection.executemany(
            "INSERT INTO title_ratings VALUES (?, ?, ?)",
            [
                ("tt0000001", 8.4, 1000),
                ("tt0000002", 8.6, 1000),
            ],
        )
        connection.execute("INSERT INTO name_unique VALUES ('nm0000001', 'Dual Role', 1970)")
        connection.executemany(
            "INSERT INTO title_principals VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("tt0000001", 1, "nm0000001", "actor", None, None),
                ("tt0000002", 1, "nm0000001", "director", None, None),
                ("tt0000002", 2, "nm0000001", "actor", None, None),
            ],
        )

        people = fetch_person_batch(connection, batch_size=10, after_person_id=None)
        self.assertEqual(len(people), 1)
        self.assertEqual(people[0].person_id, "nm0000001")
        self.assertEqual(set(people[0].category.split(",")), {"actor", "director"})

    def test_person_queries_exclude_null_birth_year(self) -> None:
        connection = duckdb.connect(database=":memory:")
        connection.execute(
            """
            CREATE TABLE title_basics (
                tconst TEXT,
                titleType TEXT,
                primaryTitle TEXT,
                startYear INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_ratings (
                tconst TEXT,
                averageRating DOUBLE,
                numVotes INTEGER
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT
            );
            """
        )
        connection.execute(
            """
            CREATE TABLE name_unique (
                nconst TEXT,
                primaryName TEXT,
                birthYear INTEGER
            );
            """
        )

        connection.execute(
            """
            INSERT INTO title_basics VALUES
                ('tt0000001', 'movie', 'A Movie', 2018),
                ('tt0000002', 'movie', 'B Movie', 2019);
            """
        )
        connection.execute(
            """
            INSERT INTO title_ratings VALUES
                ('tt0000001', 8.0, 1000),
                ('tt0000002', 7.6, 1000);
            """
        )
        connection.execute(
            """
            INSERT INTO name_unique VALUES
                ('nm0000001', 'Actor One', 1980),
                ('nm0000002', 'Actor Unknown', NULL);
            """
        )
        connection.execute(
            """
            INSERT INTO title_principals VALUES
                ('tt0000001', 1, 'nm0000001', 'actor', NULL, NULL),
                ('tt0000002', 1, 'nm0000002', 'actor', NULL, NULL);
            """
        )

        self.assertEqual(count_eligible_persons(connection), 1)
        people = fetch_person_batch(connection, batch_size=10, after_person_id=None)
        self.assertEqual([row.person_id for row in people], ["nm0000001"])


class RuntimeConfigTests(unittest.TestCase):
    def test_runtime_config_exposes_openai_and_token_limits(self) -> None:
        with patch.object(chroma_config, "OPENAI_BASE_URL", "http://localhost:11434/v1/chat/completions"), patch.object(
            chroma_config,
            "CHROMA_HOST",
            "localhost",
        ), patch.object(chroma_config, "CHROMA_PORT", 8001), patch.object(
            chroma_config,
            "INFERENCE_CONCURRENCY",
            4,
        ):
            config = load_runtime_config(batch_size=4, limit=8)

        self.assertEqual(config.batch_size, 4)
        self.assertEqual(config.limit, 8)
        self.assertEqual(config.human_max_tokens, 200)
        self.assertEqual(config.embedding_max_tokens, 250)
        self.assertEqual(config.text_generation_provider, "OpenAI-compatible API")
        self.assertEqual(config.openai_base_url, "http://localhost:11434/v1/chat/completions")
        self.assertEqual(config.inference_concurrency, 4)
        self.assertEqual(config.chroma_host, "localhost")
        self.assertEqual(config.chroma_port, 8001)

    def test_parser_defaults_to_titles_and_persons(self) -> None:
        parser = chroma_config.build_parser()
        args = parser.parse_args([])

        self.assertFalse(args.titles)
        self.assertFalse(args.persons)

    def test_runtime_config_exposes_mode_neutral_failure_threshold(self) -> None:
        config = load_runtime_config(batch_size=4, limit=8)
        self.assertTrue(hasattr(config, "max_consecutive_failures"))


class ChromaWriterTests(unittest.TestCase):
    def test_writer_uses_http_client_with_configured_endpoint(self) -> None:
        observed_host: str | None = None
        observed_port: int | None = None

        class _FakeCollection:
            def upsert(self, *, ids: list[str], documents: list[str], metadatas: list[dict[str, object]]) -> None:
                return None

        class _FakeClient:
            def get_or_create_collection(self, *, name: str) -> _FakeCollection:
                return _FakeCollection()

            def delete_collection(self, *, name: str) -> None:
                return None

        def _http_client(*, host: str, port: int) -> _FakeClient:
            nonlocal observed_host, observed_port
            observed_host = host
            observed_port = port
            return _FakeClient()

        fake_chromadb = types.ModuleType("chromadb")
        setattr(fake_chromadb, "HttpClient", _http_client)

        with patch.dict(sys.modules, {"chromadb": fake_chromadb}):
            writer = ChromaWriter(
                collection_name="titles",
                max_retries=3,
                host="localhost",
                port=8001,
            )
            writer.ensure_collection(reset=False)

        self.assertEqual(observed_host, "localhost")
        self.assertEqual(observed_port, 8001)

    def test_upsert_person_batch_omits_null_birth_year_metadata(self) -> None:
        observed_metadatas: list[dict[str, object]] = []

        class _FakeCollection:
            def upsert(self, *, ids: list[str], documents: list[str], metadatas: list[dict[str, object]]) -> None:
                observed_metadatas.extend(metadatas)

        class _FakeClient:
            def get_or_create_collection(self, *, name: str) -> _FakeCollection:
                return _FakeCollection()

            def delete_collection(self, *, name: str) -> None:
                return None

        def _http_client(*, host: str, port: int) -> _FakeClient:
            return _FakeClient()

        fake_chromadb = types.ModuleType("chromadb")
        setattr(fake_chromadb, "HttpClient", _http_client)

        records = [
            ChromaPersonSeedRecord(
                person_id="nm0000001",
                name="Known Birth Year",
                birth_year=1980,
                category="actor",
                human_description="Human one",
                embedding_description="Embedding one",
            ),
            ChromaPersonSeedRecord(
                person_id="nm0000002",
                name="Unknown Birth Year",
                birth_year=None,
                category="director",
                human_description="Human two",
                embedding_description="Embedding two",
            ),
        ]

        with patch.dict(sys.modules, {"chromadb": fake_chromadb}):
            writer = ChromaWriter(
                collection_name="persons",
                max_retries=3,
                host="localhost",
                port=8000,
            )
            writer.ensure_collection(reset=False)
            writer.upsert_person_batch(records)

        self.assertEqual(observed_metadatas[0]["birthYear"], 1980)
        self.assertNotIn("birthYear", observed_metadatas[1])


class ProgressReportingTests(unittest.TestCase):
    def test_runtime_stats_include_generation_and_chromadb_save_seconds(self) -> None:
        snapshot = ProgressSnapshot(
            processed=2,
            total=4,
            success=2,
            failed=0,
            elapsed_seconds=10.0,
            generation_seconds=7.5,
            chromadb_save_seconds=1.25,
            duckdb_query_seconds=0.45,
        )

        actual = render_runtime_stats(snapshot)

        self.assertIn("generation_sec=7.50", actual)
        self.assertIn("chromadb_save_sec=1.25", actual)
        self.assertIn("duckdb_query_sec=0.45", actual)


class ModeRunnerTests(unittest.TestCase):
    def test_run_mode_counts_dropped_records_as_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            record = TitleRecord(
                title_id="tt0000001",
                title="Alpha",
                start_year=2001,
            )

            config = load_runtime_config(batch_size=10, limit=1, run_titles=True)
            config = chroma_config.RuntimeConfig(
                duckdb_path=config.duckdb_path,
                sqlite_path=config.sqlite_path,
                chroma_host=config.chroma_host,
                chroma_port=config.chroma_port,
                collection_name_titles=config.collection_name_titles,
                collection_name_persons=config.collection_name_persons,
                batch_size=config.batch_size,
                limit=config.limit,
                selected_modes=config.selected_modes,
                text_generation_provider=config.text_generation_provider,
                model=config.model,
                openai_base_url=config.openai_base_url,
                openai_api_key=config.openai_api_key,
                human_max_tokens=config.human_max_tokens,
                embedding_max_tokens=config.embedding_max_tokens,
                inference_concurrency=config.inference_concurrency,
                max_retries=config.max_retries,
                max_consecutive_failures=config.max_consecutive_failures,
            )

            def _count_available(
                _connection: duckdb.DuckDBPyConnection,
                _resume_id: str | None,
            ) -> int:
                return 1

            def _fetch_batch(
                _connection: duckdb.DuckDBPyConnection,
                _batch_size: int,
                _resume_id: str | None,
            ) -> list[TitleRecord]:
                return [record]

            def _get_last_success_id(_store: SQLiteStore) -> None:
                return None

            def _generate_human(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return GenerationResult(
                    descriptions={record.title_id: "human"},
                    failed_ids=[],
                )

            def _generate_embedding(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return GenerationResult(
                    descriptions={},
                    failed_ids=[],
                )

            def _upsert_batch(_writer: Any, _records: list[Any]) -> None:
                return None

            def _get_summary_counts(sqlite_store: SQLiteStore) -> object:
                return sqlite_store.get_summary_counts()

            mode_runner_config_ctor = cast(Callable[..., object], getattr(chroma_main, "_ModeRunnerConfig"))
            mode_config = mode_runner_config_ctor(
                mode_name="titles",
                collection_name="titles",
                noun="titles",
                count_available=_count_available,
                fetch_batch=_fetch_batch,
                get_last_success_id=_get_last_success_id,
                generate_human=_generate_human,
                generate_embedding=_generate_embedding,
                combine_records=chroma_main.combine_title_batch_records,
                get_record_id=chroma_main.get_title_record_id,
                get_seed_record_id=chroma_main.get_title_seed_record_id,
                mark_failed_record=chroma_main.mark_title_failed_record,
                mark_success_record=chroma_main.mark_title_success_record,
                upsert_batch=_upsert_batch,
                get_summary_counts=_get_summary_counts,
                next_consecutive_failure_count=chroma_main.next_consecutive_title_failure_count,
            )

            class _FakeWriter:
                def __init__(self, **_kwargs: object) -> None:
                    return None

                def ensure_collection(self, reset: bool) -> None:
                    return None

            class _FakeProgressBar:
                def update(self, _value: int) -> None:
                    return None

                def close(self) -> None:
                    return None

                def reset(self, total: int) -> None:
                    return None

                def set_description(self, _description: str) -> None:
                    return None

            def _create_fake_overall_progress(total: int) -> _FakeProgressBar:
                return _FakeProgressBar()

            run_mode = cast(
                Callable[..., tuple[str | None, int]],
                getattr(chroma_main, "_run_mode"),
            )

            with closing(duckdb.connect(database=":memory:")) as connection, patch.object(
                chroma_main,
                "ChromaWriter",
                _FakeWriter,
            ), patch.object(
                chroma_main,
                "create_overall_progress",
                _create_fake_overall_progress,
            ), patch.object(
                chroma_main,
                "create_batch_progress",
                lambda: _FakeProgressBar(),
            ):
                stop_reason, _ = run_mode(
                    config=config,
                    store=store,
                    generation_client=cast(TextGenerationClient, object()),
                    duckdb_connection=connection,
                    reset_requested=False,
                    previous_consecutive_failures=0,
                    mode_config=mode_config,
                )

            self.assertIsNone(stop_reason)
            summary = store.get_summary_counts()
            self.assertEqual(summary.success_count, 0)
            self.assertEqual(summary.failed_count, 1)

    def test_run_mode_reports_current_round_counts_in_final_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            # Historical data from previous rounds should not affect current round output.
            store.upsert_title_success(
                title_id="tt9999998",
                title="Previous",
                start_year=2000,
                human_description="old",
                embedding_description="old",
            )

            record = TitleRecord(
                title_id="tt0000001",
                title="Alpha",
                start_year=2001,
            )

            config = load_runtime_config(batch_size=10, limit=1, run_titles=True)
            def _count_available(
                _connection: duckdb.DuckDBPyConnection,
                _resume_id: str | None,
            ) -> int:
                return 1

            def _fetch_batch(
                _connection: duckdb.DuckDBPyConnection,
                _batch_size: int,
                _resume_id: str | None,
            ) -> list[TitleRecord]:
                return [record]

            def _get_last_success_id(_store: SQLiteStore) -> None:
                return None

            def _generate_human(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return GenerationResult(
                    descriptions={record.title_id: "human"},
                    failed_ids=[],
                )

            def _generate_embedding(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return GenerationResult(
                    descriptions={record.title_id: "embedding"},
                    failed_ids=[],
                )

            def _upsert_batch(_writer: Any, _records: list[Any]) -> None:
                return None

            def _get_summary_counts(sqlite_store: SQLiteStore) -> object:
                return sqlite_store.get_summary_counts()

            mode_runner_config_ctor = cast(Callable[..., object], getattr(chroma_main, "_ModeRunnerConfig"))
            mode_config = mode_runner_config_ctor(
                mode_name="titles",
                collection_name="titles",
                noun="titles",
                count_available=_count_available,
                fetch_batch=_fetch_batch,
                get_last_success_id=_get_last_success_id,
                generate_human=_generate_human,
                generate_embedding=_generate_embedding,
                combine_records=chroma_main.combine_title_batch_records,
                get_record_id=chroma_main.get_title_record_id,
                get_seed_record_id=chroma_main.get_title_seed_record_id,
                mark_failed_record=chroma_main.mark_title_failed_record,
                mark_success_record=chroma_main.mark_title_success_record,
                upsert_batch=_upsert_batch,
                get_summary_counts=_get_summary_counts,
                next_consecutive_failure_count=chroma_main.next_consecutive_title_failure_count,
            )

            class _FakeWriter:
                def __init__(self, **_kwargs: object) -> None:
                    return None

                def ensure_collection(self, reset: bool) -> None:
                    return None

            class _FakeProgressBar:
                def update(self, _value: int) -> None:
                    return None

                def close(self) -> None:
                    return None

                def reset(self, total: int) -> None:
                    return None

                def set_description(self, _description: str) -> None:
                    return None

            def _create_fake_overall_progress(total: int) -> _FakeProgressBar:
                return _FakeProgressBar()

            run_mode = cast(
                Callable[..., tuple[str | None, int]],
                getattr(chroma_main, "_run_mode"),
            )

            output = io.StringIO()
            with closing(duckdb.connect(database=":memory:")) as connection, patch.object(
                chroma_main,
                "ChromaWriter",
                _FakeWriter,
            ), patch.object(
                chroma_main,
                "create_overall_progress",
                _create_fake_overall_progress,
            ), patch.object(
                chroma_main,
                "create_batch_progress",
                lambda: _FakeProgressBar(),
            ), redirect_stdout(output):
                stop_reason, _ = run_mode(
                    config=config,
                    store=store,
                    generation_client=cast(TextGenerationClient, object()),
                    duckdb_connection=connection,
                    reset_requested=False,
                    previous_consecutive_failures=0,
                    mode_config=mode_config,
                )

            self.assertIsNone(stop_reason)
            rendered = output.getvalue()
            self.assertIn("final_state[titles]=processed:1/1 success:1 failed:0", rendered)

    def test_run_mode_counts_failed_when_round_raises_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = Path(temp_dir) / "seed.sqlite"
            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            record = TitleRecord(
                title_id="tt0000001",
                title="Alpha",
                start_year=2001,
            )

            config = load_runtime_config(batch_size=10, limit=1, run_titles=True)
            def _count_available(
                _connection: duckdb.DuckDBPyConnection,
                _resume_id: str | None,
            ) -> int:
                return 1

            def _fetch_batch(
                _connection: duckdb.DuckDBPyConnection,
                _batch_size: int,
                _resume_id: str | None,
            ) -> list[TitleRecord]:
                return [record]

            def _get_last_success_id(_store: SQLiteStore) -> None:
                return None

            def _generate_human(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return GenerationResult(
                    descriptions={record.title_id: "human"},
                    failed_ids=[],
                )

            def _generate_embedding(
                _client: TextGenerationClient,
                _records: list[Any],
            ) -> GenerationResult:
                return (_ for _ in ()).throw(RuntimeError("boom"))

            def _upsert_batch(_writer: Any, _records: list[Any]) -> None:
                return None

            def _get_summary_counts(sqlite_store: SQLiteStore) -> object:
                return sqlite_store.get_summary_counts()

            mode_runner_config_ctor = cast(Callable[..., object], getattr(chroma_main, "_ModeRunnerConfig"))
            mode_config = mode_runner_config_ctor(
                mode_name="titles",
                collection_name="titles",
                noun="titles",
                count_available=_count_available,
                fetch_batch=_fetch_batch,
                get_last_success_id=_get_last_success_id,
                generate_human=_generate_human,
                generate_embedding=_generate_embedding,
                combine_records=chroma_main.combine_title_batch_records,
                get_record_id=chroma_main.get_title_record_id,
                get_seed_record_id=chroma_main.get_title_seed_record_id,
                mark_failed_record=chroma_main.mark_title_failed_record,
                mark_success_record=chroma_main.mark_title_success_record,
                upsert_batch=_upsert_batch,
                get_summary_counts=_get_summary_counts,
                next_consecutive_failure_count=chroma_main.next_consecutive_title_failure_count,
            )

            class _FakeWriter:
                def __init__(self, **_kwargs: object) -> None:
                    return None

                def ensure_collection(self, reset: bool) -> None:
                    return None

            class _FakeProgressBar:
                def update(self, _value: int) -> None:
                    return None

                def close(self) -> None:
                    return None

                def reset(self, total: int) -> None:
                    return None

                def set_description(self, _description: str) -> None:
                    return None

            def _create_fake_overall_progress(total: int) -> _FakeProgressBar:
                return _FakeProgressBar()

            run_mode = cast(
                Callable[..., tuple[str | None, int]],
                getattr(chroma_main, "_run_mode"),
            )

            output = io.StringIO()
            with closing(duckdb.connect(database=":memory:")) as connection, patch.object(
                chroma_main,
                "ChromaWriter",
                _FakeWriter,
            ), patch.object(
                chroma_main,
                "create_overall_progress",
                _create_fake_overall_progress,
            ), patch.object(
                chroma_main,
                "create_batch_progress",
                lambda: _FakeProgressBar(),
            ), redirect_stdout(output):
                stop_reason, _ = run_mode(
                    config=config,
                    store=store,
                    generation_client=cast(TextGenerationClient, object()),
                    duckdb_connection=connection,
                    reset_requested=False,
                    previous_consecutive_failures=0,
                    mode_config=mode_config,
                )

            self.assertIsNotNone(stop_reason)
            summary = store.get_summary_counts()
            self.assertEqual(summary.success_count, 0)
            self.assertEqual(summary.failed_count, 1)
            rendered = output.getvalue()
            self.assertIn("final_state[titles]=processed:1/1 success:0 failed:1", rendered)


class TextGenerationClientTests(unittest.TestCase):
    def test_generate_human_descriptions_supports_chat_completions_endpoint_url(self) -> None:
        titles = [
            TitleRecord(title_id="tt0000001", title="Alpha", start_year=2001),
        ]

        requests_payloads: list[dict[str, object]] = []

        class _FakeRequest:
            def __init__(self, url: str, data: bytes, headers: dict[str, str], method: str) -> None:
                self.full_url = url
                self.data = data
                self.headers = headers
                self.method = method

        class _FakeResponse:
            def __enter__(self) -> _FakeResponse:
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(
                    {
                        "choices": [
                            {
                                "message": {
                                    "content": "openai text",
                                }
                            }
                        ]
                    }
                ).encode("utf-8")

        def _fake_urlopen(request: _FakeRequest, timeout: float) -> _FakeResponse:
            payload = json.loads(request.data.decode("utf-8"))
            requests_payloads.append(payload)
            return _FakeResponse()

        fake_openai = types.ModuleType("openai")

        class _ShouldNotBeUsedOpenAI:
            def __init__(self, *, base_url: str | None = None, api_key: str | None = None) -> None:
                raise AssertionError(
                    "OpenAI SDK client should not be used for explicit /chat/completions endpoint"
                )

        setattr(fake_openai, "OpenAI", _ShouldNotBeUsedOpenAI)

        with patch.dict(sys.modules, {"openai": fake_openai}), patch(
            "scripts.chroma_seed.llm_client.urllib_request.Request",
            _FakeRequest,
        ), patch("scripts.chroma_seed.llm_client.urllib_request.urlopen", _fake_urlopen):
            client = TextGenerationClient(
                model="llama3.2:3b",
                base_url="http://localhost:11434/v1/chat/completions",
                api_key="test-key",
                max_retries=1,
                human_max_tokens=200,
                embedding_max_tokens=250,
                inference_concurrency=1,
            )

            actual = client.generate_human_descriptions(titles)

        self.assertEqual(
            actual,
            GenerationResult(
                descriptions={"tt0000001": "openai text"},
                failed_ids=[],
            ),
        )
        self.assertEqual(len(requests_payloads), 1)
        self.assertEqual(requests_payloads[0]["model"], "llama3.2:3b")
        self.assertEqual(requests_payloads[0]["max_tokens"], 200)
        self.assertEqual(requests_payloads[0]["temperature"], 0)
        messages = cast(list[dict[str, str]], requests_payloads[0]["messages"])
        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[1]["role"], "user")
        self.assertIn(" - Title: Alpha", messages[1]["content"])

    def test_generate_human_descriptions_uses_openai_and_token_limit(self) -> None:
        titles = [
            TitleRecord(title_id="tt0000001", title="Alpha", start_year=2001),
            TitleRecord(title_id="tt0000002", title="Beta", start_year=2002),
        ]

        calls: list[dict[str, object]] = []

        class _FakeCompletions:
            def create(
                self,
                *,
                model: str,
                messages: list[dict[str, str]],
                max_tokens: int,
                temperature: int,
            ) -> object:
                calls.append(
                    {
                        "model": model,
                        "messages": messages,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                    }
                )
                title = _extract_prompt_title(messages[-1]["content"])
                return _fake_chat_completion(f"human for {title}")

        class _FakeOpenAI:
            def __init__(self, *, base_url: str | None = None, api_key: str | None = None) -> None:
                self.base_url = base_url
                self.api_key = api_key
                self.chat = types.SimpleNamespace(completions=_FakeCompletions())

        fake_openai = types.ModuleType("openai")
        setattr(fake_openai, "OpenAI", _FakeOpenAI)

        with patch.dict(sys.modules, {"openai": fake_openai}):
            client = TextGenerationClient(
                model="llama3.2:3b",
                base_url="http://localhost:8002/v1",
                api_key="test-key",
                max_retries=3,
                human_max_tokens=200,
                embedding_max_tokens=250,
                inference_concurrency=2,
            )

            actual = client.generate_human_descriptions(titles)

        self.assertEqual(
            actual,
            GenerationResult(
                descriptions={
                    "tt0000001": "human for Alpha",
                    "tt0000002": "human for Beta",
                },
                failed_ids=[],
            ),
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["model"], "llama3.2:3b")
        self.assertEqual(calls[0]["max_tokens"], 200)
        self.assertEqual(calls[0]["temperature"], 0)
        first_messages = cast(list[dict[str, str]], calls[0]["messages"])
        self.assertIn(" - Title: Alpha", first_messages[-1]["content"])
        self.assertIn(" - Year: 2001", first_messages[-1]["content"])

    def test_generate_embedding_descriptions_uses_embedding_token_limit(self) -> None:
        titles = [
            TitleRecord(title_id="tt0000003", title="Gamma", start_year=2003),
        ]

        observed_max_tokens: list[int] = []

        class _FakeCompletions:
            def create(
                self,
                *,
                model: str,
                messages: list[dict[str, str]],
                max_tokens: int,
                temperature: int,
            ) -> object:
                observed_max_tokens.append(max_tokens)
                return _fake_chat_completion("structured output")

        class _FakeOpenAI:
            def __init__(self, *, base_url: str | None = None, api_key: str | None = None) -> None:
                self.base_url = base_url
                self.api_key = api_key
                self.chat = types.SimpleNamespace(completions=_FakeCompletions())

        fake_openai = types.ModuleType("openai")
        setattr(fake_openai, "OpenAI", _FakeOpenAI)

        with patch.dict(sys.modules, {"openai": fake_openai}):
            client = TextGenerationClient(
                model="llama3.2:3b",
                base_url="http://localhost:8002/v1",
                api_key="test-key",
                max_retries=3,
                human_max_tokens=200,
                embedding_max_tokens=250,
                inference_concurrency=1,
            )
            actual = client.generate_embedding_descriptions(titles)

        self.assertEqual(
            actual,
            GenerationResult(
                descriptions={"tt0000003": "structured output"},
                failed_ids=[],
            ),
        )
        self.assertEqual(observed_max_tokens, [250])

    def test_failed_title_after_retries_is_reported_without_raising(self) -> None:
        titles = [
            TitleRecord(title_id="tt0000009", title="Retry", start_year=2009),
        ]

        attempts: list[int] = []

        class _FakeCompletions:
            def create(
                self,
                *,
                model: str,
                messages: list[dict[str, str]],
                max_tokens: int,
                temperature: int,
            ) -> object:
                attempts.append(1)
                raise RuntimeError("boom")

        class _FakeOpenAI:
            def __init__(self, *, base_url: str | None = None, api_key: str | None = None) -> None:
                self.base_url = base_url
                self.api_key = api_key
                self.chat = types.SimpleNamespace(completions=_FakeCompletions())

        fake_openai = types.ModuleType("openai")
        setattr(fake_openai, "OpenAI", _FakeOpenAI)

        with patch.dict(sys.modules, {"openai": fake_openai}):
            client = TextGenerationClient(
                model="llama3.2:3b",
                base_url="http://localhost:8002/v1",
                api_key="test-key",
                max_retries=3,
                human_max_tokens=200,
                embedding_max_tokens=250,
                inference_concurrency=1,
            )

            actual = client.generate_human_descriptions(titles)

        self.assertEqual(len(attempts), 3)
        self.assertEqual(actual.descriptions, {})
        self.assertEqual(actual.failed_ids, ["tt0000009"])

    def test_retries_when_output_missing_or_empty(self) -> None:
        titles = [
            TitleRecord(title_id="tt0000011", title="One", start_year=2011),
            TitleRecord(title_id="tt0000012", title="Two", start_year=2012),
        ]

        calls: list[int] = []

        class _FakeCompletions:
            def create(
                self,
                *,
                model: str,
                messages: list[dict[str, str]],
                max_tokens: int,
                temperature: int,
            ) -> object:
                calls.append(1)
                title_line = _extract_prompt_title(messages[-1]["content"])
                if "One" in title_line:
                    return _fake_chat_completion("first")
                if len(calls) < 3:
                    return _fake_chat_completion("   ")
                return _fake_chat_completion("second")

        class _FakeOpenAI:
            def __init__(self, *, base_url: str | None = None, api_key: str | None = None) -> None:
                self.base_url = base_url
                self.api_key = api_key
                self.chat = types.SimpleNamespace(completions=_FakeCompletions())

        fake_openai = types.ModuleType("openai")
        setattr(fake_openai, "OpenAI", _FakeOpenAI)

        with patch.dict(sys.modules, {"openai": fake_openai}):
            client = TextGenerationClient(
                model="llama3.2:3b",
                base_url="http://localhost:8002/v1",
                api_key="test-key",
                max_retries=3,
                human_max_tokens=200,
                embedding_max_tokens=250,
                inference_concurrency=2,
            )
            actual = client.generate_human_descriptions(titles)

        self.assertEqual(
            actual,
            GenerationResult(
                descriptions={"tt0000011": "first", "tt0000012": "second"},
                failed_ids=[],
            ),
        )
        self.assertEqual(len(calls), 3)

    def test_generation_result_exposes_mode_neutral_failed_ids(self) -> None:
        result = GenerationResult(descriptions={}, failed_ids=[])
        self.assertTrue(hasattr(result, "failed_ids"))


def _fake_chat_completion(content: str) -> object:
    message = types.SimpleNamespace(content=content)
    choice = types.SimpleNamespace(message=message)
    return types.SimpleNamespace(choices=[choice])


def _extract_prompt_title(user_prompt: str) -> str:
    for line in user_prompt.splitlines():
        stripped = line.strip()
        if "Title:" not in stripped:
            continue
        _, _, value = stripped.partition("Title:")
        return value.strip()
    return ""


class PromptTemplatesTests(unittest.TestCase):
    def test_module_uses_two_shared_user_templates(self) -> None:
        self.assertTrue(hasattr(chroma_prompts, "_TITLE_USER_TEMPLATE"))
        self.assertTrue(hasattr(chroma_prompts, "_PERSON_USER_TEMPLATE"))

    def test_title_prompt_builders_share_user_template_output_shape(self) -> None:
        title = TitleRecord(title_id="tt0000013", title="Echo", start_year=2013)

        _, human_user = chroma_prompts.build_title_description_prompt(title)
        _, embedding_user = chroma_prompts.build_title_embedding_prompt(title)

        self.assertEqual(human_user, embedding_user)

    def test_person_prompt_builders_share_user_template_output_shape(self) -> None:
        person = PersonRecord(
            person_id="nm0000013",
            name="Casey",
            birth_year=1988,
            category="actor",
        )

        _, human_user = chroma_prompts.build_person_description_prompt(person)
        _, embedding_user = chroma_prompts.build_person_embedding_prompt(person)

        self.assertEqual(human_user, embedding_user)

    def test_person_user_template_uses_spec_birth_year_placeholder(self) -> None:
        person_template = cast(str, getattr(chroma_prompts, "_PERSON_USER_TEMPLATE"))
        self.assertIn("{birthYear}", person_template)


class ModeHelpersModuleTests(unittest.TestCase):
    def test_mode_helpers_module_exports_auxiliary_functions(self) -> None:
        module = importlib.import_module("scripts.chroma_seed.mode_helpers")

        self.assertTrue(hasattr(module, "combine_title_batch_records"))
        self.assertTrue(hasattr(module, "combine_person_batch_records"))
        self.assertTrue(hasattr(module, "persist_generation_failures"))
        self.assertTrue(hasattr(module, "next_consecutive_title_failure_count"))
        self.assertTrue(hasattr(module, "next_consecutive_person_failure_count"))


if __name__ == "__main__":
    unittest.main()
