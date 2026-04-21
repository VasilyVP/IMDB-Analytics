from __future__ import annotations

import sqlite3
import json
import types
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from typing import cast
from unittest.mock import patch

import duckdb

_REPO_ROOT = Path(__file__).resolve().parents[2]
import sys

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.chroma_seed.duckdb_reader import (  # noqa: E402
    count_eligible_titles,
    fetch_title_batch,
)
import scripts.chroma_seed.config as chroma_config  # noqa: E402
from scripts.chroma_seed.chroma_writer import ChromaWriter  # noqa: E402
from scripts.chroma_seed.llm_client import GenerationResult, TextGenerationClient  # noqa: E402
from scripts.chroma_seed.models import TitleRecord  # noqa: E402
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
            self.assertIn("seed_records", table_names)
            self.assertIn("seed_failures", table_names)

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
                    "SELECT title_id, phase, attempt, error_message FROM seed_failures"
                ).fetchall()

            self.assertEqual(len(failure_rows), 1)
            self.assertEqual(
                failure_rows[0],
                ("tt1234567", "human_generation", 3, "generation failed"),
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

        self.assertEqual(count_eligible_titles(connection), 2)

        rows = fetch_title_batch(connection, batch_size=10, after_title_id=None)
        self.assertEqual([row.title_id for row in rows], ["tt0000001", "tt0000002"])

        resumed_rows = fetch_title_batch(
            connection,
            batch_size=10,
            after_title_id="tt0000001",
        )
        self.assertEqual([row.title_id for row in resumed_rows], ["tt0000002"])


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
                failed_title_ids=[],
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
                failed_title_ids=[],
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
                failed_title_ids=[],
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
        self.assertEqual(actual.failed_title_ids, ["tt0000009"])

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
                failed_title_ids=[],
            ),
        )
        self.assertEqual(len(calls), 3)


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


if __name__ == "__main__":
    unittest.main()
