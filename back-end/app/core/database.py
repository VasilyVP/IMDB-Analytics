from __future__ import annotations

import duckdb
from neo4j import GraphDatabase  # type: ignore[import-untyped]
from neo4j import Driver

from app.core.config import get_settings

_duckdb_conn: duckdb.DuckDBPyConnection | None = None
_neo4j_driver: Driver | None = None


def init_db() -> None:
    global _duckdb_conn, _neo4j_driver
    settings = get_settings()
    _duckdb_conn = duckdb.connect(settings.DUCKDB_PATH)
    _neo4j_driver = GraphDatabase.driver(  # type: ignore[reportUnknownMemberType]
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )


def close_db() -> None:
    global _duckdb_conn, _neo4j_driver
    if _duckdb_conn is not None:
        _duckdb_conn.close()
        _duckdb_conn = None
    if _neo4j_driver is not None:
        _neo4j_driver.close()
        _neo4j_driver = None


def get_duckdb() -> duckdb.DuckDBPyConnection:
    if _duckdb_conn is None:
        raise RuntimeError("Database not initialised — call init_db() first")
    return _duckdb_conn


def get_neo4j() -> Driver:
    if _neo4j_driver is None:
        raise RuntimeError("Neo4j driver not initialised — call init_db() first")
    return _neo4j_driver
