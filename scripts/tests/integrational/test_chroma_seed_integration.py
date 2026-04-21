from __future__ import annotations

import os
import socket
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import duckdb
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]
import sys

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

load_dotenv(dotenv_path=_REPO_ROOT / ".env.test", override=True)

import scripts.chroma_seed.config as chroma_config  # noqa: E402
from scripts.chroma_seed.chroma_writer import ChromaWriter  # noqa: E402
from scripts.chroma_seed.duckdb_reader import count_eligible_titles, fetch_title_batch  # noqa: E402
from scripts.chroma_seed.llm_client import TextGenerationClient  # noqa: E402
from scripts.chroma_seed.models import ChromaSeedRecord  # noqa: E402
from scripts.chroma_seed.sqlite_store import SQLiteStore  # noqa: E402


class ChromaSeedScriptIntegrationTests(unittest.TestCase):
    def test_main_seeds_ten_titles_into_test_sqlite_and_chromadb_collection(self) -> None:
        settings = _load_integration_test_settings()

        openai_host, openai_port = _extract_host_port(settings.openai_base_url)
        if not _is_tcp_endpoint_available(openai_host, openai_port):
            self.fail(
                f"OpenAI-compatible endpoint is unavailable at {openai_host}:{openai_port}"
            )
        if not _is_tcp_endpoint_available(settings.chroma_host, settings.chroma_port):
            self.fail(
                "ChromaDB endpoint is unavailable at "
                f"{settings.chroma_host}:{settings.chroma_port}"
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            duckdb_path = temp_path / settings.duckdb_filename
            sqlite_path = temp_path / settings.sqlite_filename
            collection_name = f"titles_test_{uuid4().hex}"

            _create_test_duckdb_with_eligible_titles(
                duckdb_path,
                eligible_count=settings.eligible_titles,
            )

            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            with closing(duckdb.connect(str(duckdb_path), read_only=True)) as duckdb_connection:
                self.assertEqual(count_eligible_titles(duckdb_connection), settings.eligible_titles)
                titles = fetch_title_batch(
                    duckdb_connection,
                    batch_size=settings.batch_size,
                    after_title_id=None,
                )

            generation_client = TextGenerationClient(
                model=chroma_config.TEXT_GENERATION_MODEL,
                base_url=settings.openai_base_url,
                api_key=settings.openai_api_key,
                max_retries=settings.max_retries,
                human_max_tokens=chroma_config.HUMAN_MAX_TOKENS,
                embedding_max_tokens=chroma_config.EMBEDDING_MAX_TOKENS,
                inference_concurrency=settings.inference_concurrency,
            )
            human_result = generation_client.generate_human_descriptions(titles)
            if human_result.failed_title_ids:
                errors = sorted(set(human_result.failure_messages.values()))
                self.fail(
                    "OpenAI-compatible generation failed for integration run: "
                    f"{'; '.join(errors)}"
                )

            titles_with_human = [
                title for title in titles if title.title_id in human_result.descriptions
            ]
            embedding_result = generation_client.generate_embedding_descriptions(titles_with_human)
            self.assertEqual(embedding_result.failed_title_ids, [])

            records_to_write = [
                ChromaSeedRecord(
                    title_id=title.title_id,
                    title=title.title,
                    start_year=title.start_year,
                    human_description=human_result.descriptions[title.title_id],
                    embedding_description=embedding_result.descriptions[title.title_id],
                )
                for title in titles
                if title.title_id in human_result.descriptions
                and title.title_id in embedding_result.descriptions
            ]

            writer = ChromaWriter(
                collection_name=collection_name,
                max_retries=settings.max_retries,
                host=settings.chroma_host,
                port=settings.chroma_port,
            )
            writer.ensure_collection(reset=True)
            writer.upsert_batch(records_to_write)

            for record in records_to_write:
                store.upsert_success(
                    title_id=record.title_id,
                    title=record.title,
                    start_year=record.start_year,
                    human_description=record.human_description,
                    embedding_description=record.embedding_description,
                )

            summary = store.get_summary_counts()
            self.assertEqual(summary.success_count, settings.eligible_titles)
            self.assertEqual(summary.failed_count, 0)

            with closing(sqlite3.connect(sqlite_path)) as connection:
                successful_records = connection.execute(
                    "SELECT COUNT(*) FROM seed_records WHERE status = 'success'"
                ).fetchone()

            self.assertIsNotNone(successful_records)
            self.assertEqual(int(successful_records[0]), settings.eligible_titles)
            self.assertEqual(len(records_to_write), settings.eligible_titles)
            self.assertEqual(records_to_write[0].title_id, "tt0000001")


@dataclass(frozen=True, slots=True)
class _IntegrationTestSettings:
    duckdb_filename: str
    sqlite_filename: str
    eligible_titles: int
    batch_size: int
    inference_concurrency: int
    max_retries: int
    chroma_host: str
    chroma_port: int
    openai_base_url: str
    openai_api_key: str | None


def _load_integration_test_settings() -> _IntegrationTestSettings:
    eligible_titles = _read_int_env("CHROMA_TEST_ELIGIBLE_TITLES", default=1)
    batch_size = _read_int_env("CHROMA_TEST_BATCH_SIZE", default=eligible_titles)

    return _IntegrationTestSettings(
        duckdb_filename=os.getenv("CHROMA_TEST_DUCKDB_FILENAME", "imdb.duckdb"),
        sqlite_filename=os.getenv("CHROMA_TEST_SQLITE_FILENAME", "seed.sqlite"),
        eligible_titles=eligible_titles,
        batch_size=batch_size,
        inference_concurrency=_read_int_env("CHROMA_TEST_INFERENCE_CONCURRENCY", default=1),
        max_retries=_read_int_env("CHROMA_TEST_MAX_RETRIES", default=1),
        chroma_host=os.getenv("CHROMA_HOST", chroma_config.CHROMA_HOST),
        chroma_port=_read_int_env("CHROMA_PORT", default=chroma_config.CHROMA_PORT),
        openai_base_url=os.getenv("OPENAI_BASE_URL", chroma_config.OPENAI_BASE_URL),
        openai_api_key=os.getenv("OPENAI_API_KEY", chroma_config.OPENAI_API_KEY),
    )


def _read_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _create_test_duckdb_with_eligible_titles(
    duckdb_path: Path,
    eligible_count: int,
) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
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

        basics_rows: list[tuple[str, str, str, int]] = []
        ratings_rows: list[tuple[str, float, int]] = []
        for index in range(1, eligible_count + 1):
            title_id = f"tt{index:07d}"
            basics_rows.append((title_id, "movie", f"Title {index}", 2018))
            ratings_rows.append((title_id, 8.1, 1000))

        connection.executemany(
            "INSERT INTO title_basics VALUES (?, ?, ?, ?)",
            basics_rows,
        )
        connection.executemany(
            "INSERT INTO title_ratings VALUES (?, ?, ?)",
            ratings_rows,
        )
    finally:
        connection.close()


def _extract_host_port(base_url: str) -> tuple[str, int]:
    parsed = urlparse(base_url)
    host = parsed.hostname or "localhost"
    if parsed.port is not None:
        return host, parsed.port
    if parsed.scheme == "https":
        return host, 443
    return host, 80


def _is_tcp_endpoint_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


if __name__ == "__main__":
    unittest.main()
