from __future__ import annotations

from collections.abc import Callable

import duckdb

from app.core.actions import ensure_duckdb_analytics_views

StartupAction = Callable[[duckdb.DuckDBPyConnection], None]

STARTUP_ACTIONS: tuple[tuple[str, StartupAction], ...] = (
    ("ensure_duckdb_analytics_views", ensure_duckdb_analytics_views.execute),
)


def run_startup_actions(
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    for action_name, action_execute in STARTUP_ACTIONS:
        try:
            action_execute(duckdb_conn)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Startup action '{action_name}' failed") from exc
