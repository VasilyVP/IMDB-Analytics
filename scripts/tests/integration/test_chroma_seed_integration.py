from __future__ import annotations

import os
import socket
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast
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
from scripts.chroma_seed.duckdb_reader import (  # noqa: E402
    count_eligible_persons,
    count_eligible_titles,
    fetch_person_batch,
    fetch_title_batch,
)
from scripts.chroma_seed.llm_client import TextGenerationClient  # noqa: E402
from scripts.chroma_seed.models import ChromaPersonSeedRecord, ChromaSeedRecord  # noqa: E402
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
                first_title_row = duckdb_connection.execute(
                    "SELECT primaryTitle, startYear FROM title_basics ORDER BY tconst LIMIT 1"
                ).fetchone()
                if first_title_row is None:
                    self.fail("Expected at least one title row in test DuckDB")

                first_title = cast(str, first_title_row[0])
                first_start_year = cast(int, first_title_row[1])
                self.assertEqual(first_title, "Glass")
                self.assertEqual(first_start_year, 2019)

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
            if human_result.failed_ids:
                errors = sorted(set(human_result.failure_messages.values()))
                self.fail(
                    "OpenAI-compatible generation failed for integration run: "
                    f"{'; '.join(errors)}"
                )

            titles_with_human = [
                title for title in titles if title.title_id in human_result.descriptions
            ]
            embedding_result = generation_client.generate_embedding_descriptions(titles_with_human)
            self.assertEqual(embedding_result.failed_ids, [])

            if settings.eligible_titles == 1 and titles:
                title = titles[0]
                title_id = title.title_id
                print(f"\n--- Title: {title.title} ({title_id}) ---")
                print(
                    "[input]     "
                    f"title={title.title!r}, start_year={title.start_year}, title_id={title_id}"
                )
                print(f"[human]     {human_result.descriptions.get(title_id)}")
                print(f"[embedding] {embedding_result.descriptions.get(title_id)}", flush=True)

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
                    "SELECT COUNT(*) FROM seed_titles WHERE status = 'success'"
                ).fetchone()

            self.assertIsNotNone(successful_records)
            self.assertEqual(int(successful_records[0]), settings.eligible_titles)
            self.assertEqual(len(records_to_write), settings.eligible_titles)
            self.assertEqual(records_to_write[0].title_id, "tt0000001")

    def test_main_seeds_persons_into_test_sqlite_and_chromadb_collection(self) -> None:
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
            collection_name = f"persons_test_{uuid4().hex}"

            _create_test_duckdb_with_eligible_persons(
                duckdb_path,
                eligible_count=settings.eligible_persons,
            )

            store = SQLiteStore(sqlite_path)
            store.initialize_schema()

            with closing(duckdb.connect(str(duckdb_path), read_only=True)) as duckdb_connection:
                first_person_row = duckdb_connection.execute(
                    "SELECT primaryName, birthYear FROM name_unique ORDER BY nconst LIMIT 1"
                ).fetchone()
                if first_person_row is None:
                    self.fail("Expected at least one person row in test DuckDB")

                first_person_name = cast(str, first_person_row[0])
                first_person_birth_year = cast(int, first_person_row[1])
                self.assertEqual(first_person_name, "Bruce Willis")
                self.assertEqual(first_person_birth_year, 1955)

                self.assertEqual(
                    count_eligible_persons(duckdb_connection), settings.eligible_persons
                )
                persons = fetch_person_batch(
                    duckdb_connection,
                    batch_size=settings.batch_size,
                    after_person_id=None,
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
            human_result = generation_client.generate_person_human_descriptions(persons)
            if human_result.failed_ids:
                errors = sorted(set(human_result.failure_messages.values()))
                self.fail(
                    "OpenAI-compatible generation failed for integration run: "
                    f"{'; '.join(errors)}"
                )

            persons_with_human = [
                person for person in persons if person.person_id in human_result.descriptions
            ]
            embedding_result = generation_client.generate_person_embedding_descriptions(
                persons_with_human
            )
            self.assertEqual(embedding_result.failed_ids, [])

            if settings.eligible_persons == 1 and persons:
                person = persons[0]
                person_id = person.person_id
                print(f"\n--- Person: {person.name} ({person_id}) ---")
                print(
                    "[input]     "
                    f"name={person.name!r}, birth_year={person.birth_year}, "
                    f"category={person.category!r}, person_id={person_id}"
                )
                print(f"[human]     {human_result.descriptions.get(person_id)}")
                print(f"[embedding] {embedding_result.descriptions.get(person_id)}", flush=True)

            records_to_write = [
                ChromaPersonSeedRecord(
                    person_id=person.person_id,
                    name=person.name,
                    birth_year=person.birth_year,
                    category=person.category,
                    human_description=human_result.descriptions[person.person_id],
                    embedding_description=embedding_result.descriptions[person.person_id],
                )
                for person in persons
                if person.person_id in human_result.descriptions
                and person.person_id in embedding_result.descriptions
            ]

            writer = ChromaWriter(
                collection_name=collection_name,
                max_retries=settings.max_retries,
                host=settings.chroma_host,
                port=settings.chroma_port,
            )
            writer.ensure_collection(reset=True)
            writer.upsert_person_batch(records_to_write)

            for record in records_to_write:
                store.upsert_person_success(
                    person_id=record.person_id,
                    name=record.name,
                    birth_year=record.birth_year,
                    category=record.category,
                    human_description=record.human_description,
                    embedding_description=record.embedding_description,
                )

            summary = store.get_person_summary_counts()
            self.assertEqual(summary.success_count, settings.eligible_persons)
            self.assertEqual(summary.failed_count, 0)

            with closing(sqlite3.connect(sqlite_path)) as connection:
                successful_records = connection.execute(
                    "SELECT COUNT(*) FROM seed_persons WHERE status = 'success'"
                ).fetchone()

            self.assertIsNotNone(successful_records)
            self.assertEqual(int(successful_records[0]), settings.eligible_persons)
            self.assertEqual(len(records_to_write), settings.eligible_persons)
            self.assertEqual(records_to_write[0].person_id, "nm0000001")


@dataclass(frozen=True, slots=True)
class _IntegrationTestSettings:
    duckdb_filename: str
    sqlite_filename: str
    eligible_titles: int
    eligible_persons: int
    batch_size: int
    inference_concurrency: int
    max_retries: int
    chroma_host: str
    chroma_port: int
    openai_base_url: str
    openai_api_key: str | None


def _load_integration_test_settings() -> _IntegrationTestSettings:
    eligible_titles = _read_int_env("CHROMA_TEST_ELIGIBLE_TITLES", default=1)
    eligible_persons = _read_int_env("CHROMA_TEST_ELIGIBLE_PERSONS", default=1)
    batch_size = _read_int_env("CHROMA_TEST_BATCH_SIZE", default=max(eligible_titles, eligible_persons))

    return _IntegrationTestSettings(
        duckdb_filename=os.getenv("CHROMA_TEST_DUCKDB_FILENAME", "imdb.duckdb"),
        sqlite_filename=os.getenv("CHROMA_TEST_SQLITE_FILENAME", "seed.sqlite"),
        eligible_titles=eligible_titles,
        eligible_persons=eligible_persons,
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


def _create_test_duckdb_with_eligible_persons(
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
        connection.execute(
            """
            CREATE TABLE title_principals (
                tconst TEXT,
                nconst TEXT,
                category TEXT
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

        basics_rows: list[tuple[str, str, str, int]] = []
        ratings_rows: list[tuple[str, float, int]] = []
        principals_rows: list[tuple[str, str, str]] = []
        name_rows: list[tuple[str, str, int]] = []
        if eligible_count > 0:
            basics_rows.append(("tt0000001", "movie", "Glass", 2019))
            ratings_rows.append(("tt0000001", 8.1, 1000))
            principals_rows.append(("tt0000001", "nm0000001", "actor"))
            name_rows.append(("nm0000001", "Bruce Willis", 1955))

        for index in range(2, eligible_count + 1):
            generated_index = index
            title_id = f"tt{generated_index:07d}"
            person_id = f"nm{generated_index:07d}"
            basics_rows.append((title_id, "movie", f"Title {index}", 2018))
            ratings_rows.append((title_id, 8.1, 1000))
            principals_rows.append((title_id, person_id, "actor"))
            name_rows.append((person_id, f"Person {index}", 1980))

        connection.executemany(
            "INSERT INTO title_basics VALUES (?, ?, ?, ?)",
            basics_rows,
        )
        connection.executemany(
            "INSERT INTO title_ratings VALUES (?, ?, ?)",
            ratings_rows,
        )
        connection.executemany(
            "INSERT INTO title_principals VALUES (?, ?, ?)",
            principals_rows,
        )
        connection.executemany(
            "INSERT INTO name_unique VALUES (?, ?, ?)",
            name_rows,
        )
    finally:
        connection.close()


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
        if eligible_count > 0:
            basics_rows.append(("tt0000001", "movie", "Glass", 2019))
            ratings_rows.append(("tt0000001", 8.1, 1000))

        for index in range(2, eligible_count + 1):
            generated_index = index
            title_id = f"tt{generated_index:07d}"
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
