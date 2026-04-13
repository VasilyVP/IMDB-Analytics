from __future__ import annotations

import unittest
from typing import cast
from unittest.mock import patch

import duckdb

from app.core.startup import run_startup_actions
from app.core.actions import ensure_duckdb_analytics_views


class _FakeDuckDBConnection:
    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.executed_sql: list[str] = []

    def execute(self, sql: str) -> None:
        if self.fail_on is not None and self.fail_on in sql:
            raise RuntimeError("sql failure")
        self.executed_sql.append(sql)


class StartupProcedureTests(unittest.TestCase):
    def test_run_startup_actions_executes_actions_in_order(self) -> None:
        calls: list[str] = []

        def first_action(_: object) -> None:
            calls.append("first")

        def second_action(_: object) -> None:
            calls.append("second")

        with patch(
            "app.core.startup.STARTUP_ACTIONS",
            (("first", first_action), ("second", second_action)),
        ):
            run_startup_actions(cast(duckdb.DuckDBPyConnection, _FakeDuckDBConnection()))

        self.assertEqual(calls, ["first", "second"])

    def test_run_startup_actions_stops_on_first_failure(self) -> None:
        calls: list[str] = []

        def first_action(_: object) -> None:
            calls.append("first")
            raise RuntimeError("boom")

        def second_action(_: object) -> None:
            calls.append("second")

        with self.assertRaises(RuntimeError):
            with patch(
                "app.core.startup.STARTUP_ACTIONS",
                (("first", first_action), ("second", second_action)),
            ):
                run_startup_actions(cast(duckdb.DuckDBPyConnection, _FakeDuckDBConnection()))

        self.assertEqual(calls, ["first"])


class EnsureDuckDBAnalyticsViewsTests(unittest.TestCase):
    def test_execute_creates_all_required_views(self) -> None:
        conn = _FakeDuckDBConnection()

        ensure_duckdb_analytics_views.execute(cast(duckdb.DuckDBPyConnection, conn))

        executed = "\n".join(conn.executed_sql)
        self.assertIn("CREATE OR REPLACE VIEW top_rated_titles AS", executed)
        self.assertIn("CREATE OR REPLACE VIEW most_popular_titles AS", executed)
        self.assertIn("CREATE OR REPLACE VIEW top_rated_popular_titles AS", executed)

    def test_execute_raises_when_required_table_is_missing(self) -> None:
        conn = _FakeDuckDBConnection(fail_on="title_ratings")

        with self.assertRaises(RuntimeError):
            ensure_duckdb_analytics_views.execute(cast(duckdb.DuckDBPyConnection, conn))


if __name__ == "__main__":
    unittest.main()
