from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from .models import SummaryCounts


class SQLiteStore:
    def __init__(self, sqlite_path: Path) -> None:
        self._sqlite_path = sqlite_path

    def initialize_schema(self) -> None:
        self._sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS seed_titles (
                    title_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    start_year INTEGER,
                    human_description TEXT,
                    embedding_description TEXT,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS seed_persons (
                    person_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    birth_year INTEGER,
                    category TEXT NOT NULL,
                    human_description TEXT,
                    embedding_description TEXT,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS seed_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    error_message TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def has_title_records(self) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT COUNT(*) FROM seed_titles").fetchone()
        return bool(row and int(row[0]) > 0)

    def has_person_records(self) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT COUNT(*) FROM seed_persons").fetchone()
        return bool(row and int(row[0]) > 0)

    def has_records(self) -> bool:
        return self.has_title_records() or self.has_person_records()

    def clear_all(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("DELETE FROM seed_failures")
            connection.execute("DELETE FROM seed_titles")
            connection.execute("DELETE FROM seed_persons")
            connection.commit()

    def clear_titles(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("DELETE FROM seed_titles")
            connection.commit()

    def clear_persons(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("DELETE FROM seed_persons")
            connection.commit()

    def get_last_success_title_id(self) -> str | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT MAX(title_id) FROM seed_titles WHERE status = 'success'"
            ).fetchone()
        if row is None:
            return None
        return row[0]

    def get_last_success_person_id(self) -> str | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT MAX(person_id) FROM seed_persons WHERE status = 'success'"
            ).fetchone()
        if row is None:
            return None
        return row[0]

    def upsert_title_success(
        self,
        title_id: str,
        title: str,
        start_year: int,
        human_description: str,
        embedding_description: str,
    ) -> None:
        now = _utc_now_iso()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO seed_titles (
                    title_id,
                    title,
                    start_year,
                    human_description,
                    embedding_description,
                    status,
                    last_error,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, 'success', NULL, ?)
                ON CONFLICT(title_id) DO UPDATE SET
                    title = excluded.title,
                    start_year = excluded.start_year,
                    human_description = excluded.human_description,
                    embedding_description = excluded.embedding_description,
                    status = 'success',
                    last_error = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    title_id,
                    title,
                    start_year,
                    human_description,
                    embedding_description,
                    now,
                ),
            )
            connection.commit()

    def upsert_person_success(
        self,
        person_id: str,
        name: str,
        birth_year: int | None,
        category: str,
        human_description: str,
        embedding_description: str,
    ) -> None:
        now = _utc_now_iso()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO seed_persons (
                    person_id,
                    name,
                    birth_year,
                    category,
                    human_description,
                    embedding_description,
                    status,
                    last_error,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'success', NULL, ?)
                ON CONFLICT(person_id) DO UPDATE SET
                    name = excluded.name,
                    birth_year = excluded.birth_year,
                    category = excluded.category,
                    human_description = excluded.human_description,
                    embedding_description = excluded.embedding_description,
                    status = 'success',
                    last_error = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    person_id,
                    name,
                    birth_year,
                    category,
                    human_description,
                    embedding_description,
                    now,
                ),
            )
            connection.commit()

    def mark_title_failed(
        self,
        title_id: str,
        title: str,
        start_year: int,
        phase: str,
        attempt: int,
        error_message: str,
    ) -> None:
        now = _utc_now_iso()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO seed_titles (
                    title_id,
                    title,
                    start_year,
                    human_description,
                    embedding_description,
                    status,
                    last_error,
                    updated_at
                ) VALUES (?, ?, ?, NULL, NULL, 'failed', ?, ?)
                ON CONFLICT(title_id) DO UPDATE SET
                    title = excluded.title,
                    start_year = excluded.start_year,
                    status = 'failed',
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (title_id, title, start_year, error_message, now),
            )
            connection.execute(
                """
                INSERT INTO seed_failures (record_id, phase, attempt, error_message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (title_id, phase, attempt, error_message, now),
            )
            connection.commit()

    def mark_person_failed(
        self,
        person_id: str,
        name: str,
        birth_year: int | None,
        category: str,
        phase: str,
        attempt: int,
        error_message: str,
    ) -> None:
        now = _utc_now_iso()
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO seed_persons (
                    person_id,
                    name,
                    birth_year,
                    category,
                    human_description,
                    embedding_description,
                    status,
                    last_error,
                    updated_at
                ) VALUES (?, ?, ?, ?, NULL, NULL, 'failed', ?, ?)
                ON CONFLICT(person_id) DO UPDATE SET
                    name = excluded.name,
                    birth_year = excluded.birth_year,
                    category = excluded.category,
                    status = 'failed',
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (person_id, name, birth_year, category, error_message, now),
            )
            connection.execute(
                """
                INSERT INTO seed_failures (record_id, phase, attempt, error_message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (person_id, phase, attempt, error_message, now),
            )
            connection.commit()

    def get_summary_counts(self) -> SummaryCounts:
        with closing(self._connect()) as connection:
            success_row = connection.execute(
                "SELECT COUNT(*) FROM seed_titles WHERE status = 'success'"
            ).fetchone()
            failed_row = connection.execute(
                "SELECT COUNT(*) FROM seed_titles WHERE status = 'failed'"
            ).fetchone()

        return SummaryCounts(
            success_count=int(success_row[0]) if success_row is not None else 0,
            failed_count=int(failed_row[0]) if failed_row is not None else 0,
        )

    def get_person_summary_counts(self) -> SummaryCounts:
        with closing(self._connect()) as connection:
            success_row = connection.execute(
                "SELECT COUNT(*) FROM seed_persons WHERE status = 'success'"
            ).fetchone()
            failed_row = connection.execute(
                "SELECT COUNT(*) FROM seed_persons WHERE status = 'failed'"
            ).fetchone()

        return SummaryCounts(
            success_count=int(success_row[0]) if success_row is not None else 0,
            failed_count=int(failed_row[0]) if failed_row is not None else 0,
        )

    # Backward-compatible wrappers.
    def upsert_success(
        self,
        title_id: str,
        title: str,
        start_year: int,
        human_description: str,
        embedding_description: str,
    ) -> None:
        self.upsert_title_success(
            title_id=title_id,
            title=title,
            start_year=start_year,
            human_description=human_description,
            embedding_description=embedding_description,
        )

    def mark_failed(
        self,
        title_id: str,
        title: str,
        start_year: int,
        phase: str,
        attempt: int,
        error_message: str,
    ) -> None:
        self.mark_title_failed(
            title_id=title_id,
            title=title,
            start_year=start_year,
            phase=phase,
            attempt=attempt,
            error_message=error_message,
        )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._sqlite_path)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()
