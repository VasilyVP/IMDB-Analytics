"""
Seed a local Neo4j instance from imdb.duckdb.

Usage:
    python scripts/neo4j_seed.py [--limit N]

Requires .env with NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
Wipes the database before seeding.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any, Generator, LiteralString, cast

import duckdb
from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase, Query, Session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
DUCKDB_PATH = ROOT / "back-end" / "data" / "imdb.duckdb"
BATCH_SIZE = 5_000

# Substitution map — categories not listed fall back to UPPER_SNAKE_CASE of the category name
CATEGORY_TO_REL_TYPE: dict[str, str] = {
    "actor": "ACTED_IN",
    "actress": "ACTED_IN",
    "director": "DIRECTED",
    "writer": "WROTE",
    "producer": "PRODUCED",
    "composer": "COMPOSED",
    "editor": "EDITED",
    "cinematographer": "SHOT",
    "production_designer": "DESIGNED",
    "casting_director": "CAST",
    "self": "APPEARED_IN",
    "archive_footage": "APPEARED_IN",
    "archive_sound": "APPEARED_IN",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IMDB_NULL = r"\N"


def _null(value: Any) -> Any:
    """Return None if value is IMDb's null sentinel, otherwise return the value."""
    if value is None or value == _IMDB_NULL:
        return None
    return value


def _int(value: Any) -> int | None:
    v = _null(value)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _float(value: Any) -> float | None:
    v = _null(value)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _batches(rows: Any, size: int) -> Generator[list[Any], None, None]:
    batch: list[Any] = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# neo4j connection
# ---------------------------------------------------------------------------


def load_env():
    load_dotenv(ROOT / ".env")
    uri = os.environ["NEO4J_URI"]
    user = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]
    return uri, user, password


def get_driver(uri: str, user: str, password: str) -> Driver:
    driver = GraphDatabase.driver(uri, auth=(user, password))  # type: ignore[misc]
    driver.verify_connectivity()  # type: ignore[misc]
    print(f"Connected to Neo4j at {uri}")
    return driver


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def wipe_database(session: Session) -> None:
    print("Wiping database...")
    t = time.time()
    session.run(
        "MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"
    )
    print(f"  Done ({time.time() - t:.1f}s)")


def create_schema(session: Session) -> None:
    print("Creating constraints and indexes...")

    ddl = [
        # Uniqueness constraints (imply indexes)
        "CREATE CONSTRAINT person_nconst IF NOT EXISTS FOR (p:Person) REQUIRE p.nconst IS UNIQUE",
        "CREATE CONSTRAINT title_tconst  IF NOT EXISTS FOR (t:Title)  REQUIRE t.tconst IS UNIQUE",
        # Person
        "CREATE TEXT INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.primaryName)",
        # Title
        "CREATE TEXT INDEX title_name      IF NOT EXISTS FOR (t:Title) ON (t.primaryTitle)",
        "CREATE INDEX      title_year      IF NOT EXISTS FOR (t:Title) ON (t.startYear)",
        "CREATE INDEX      title_genres    IF NOT EXISTS FOR (t:Title) ON (t.genres)",
        "CREATE INDEX      title_rating    IF NOT EXISTS FOR (t:Title) ON (t.averageRating)",
        "CREATE INDEX      title_type      IF NOT EXISTS FOR (t:Title) ON (t.titleType)",
        # Relationship property indexes (category, one per rel type)
        "CREATE INDEX rel_acted_in    IF NOT EXISTS FOR ()-[r:ACTED_IN]-()    ON (r.category)",
        "CREATE INDEX rel_directed     IF NOT EXISTS FOR ()-[r:DIRECTED]-()    ON (r.category)",
        "CREATE INDEX rel_wrote        IF NOT EXISTS FOR ()-[r:WROTE]-()       ON (r.category)",
        "CREATE INDEX rel_produced     IF NOT EXISTS FOR ()-[r:PRODUCED]-()    ON (r.category)",
        "CREATE INDEX rel_composed     IF NOT EXISTS FOR ()-[r:COMPOSED]-()    ON (r.category)",
        "CREATE INDEX rel_edited       IF NOT EXISTS FOR ()-[r:EDITED]-()      ON (r.category)",
        "CREATE INDEX rel_shot         IF NOT EXISTS FOR ()-[r:SHOT]-()        ON (r.category)",
        "CREATE INDEX rel_designed     IF NOT EXISTS FOR ()-[r:DESIGNED]-()    ON (r.category)",
        "CREATE INDEX rel_cast         IF NOT EXISTS FOR ()-[r:CAST]-()        ON (r.category)",
        "CREATE INDEX rel_appeared_in  IF NOT EXISTS FOR ()-[r:APPEARED_IN]-() ON (r.category)",
    ]

    for stmt in ddl:
        session.run(Query(cast(LiteralString, stmt)))

    print(f"  {len(ddl)} constraints/indexes created")


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def seed_persons(session: Session, con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    print("Seeding Person nodes...")
    t = time.time()
    if limit is not None:
        query = (
            "SELECT nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles"
            " FROM name_unique"
            " WHERE nconst IN ("
            "  SELECT DISTINCT nconst FROM title_principals"
            f"  WHERE tconst IN (SELECT tconst FROM title_basics ORDER BY tconst LIMIT {limit})"
            " )"
        )
    else:
        query = (
            "SELECT nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles"
            " FROM name_unique"
        )
    rows = con.execute(query).fetchall()

    total = 0
    for batch in _batches(rows, BATCH_SIZE):
        data: list[dict[str, Any]] = [
            {
                "nconst": r[0],
                "primaryName": _null(r[1]),
                "birthYear": _int(r[2]),
                "deathYear": _int(r[3]),
                "primaryProfession": _null(r[4]),
                "knownForTitles": _null(r[5]),
            }
            for r in batch
        ]
        result = session.run(
            "UNWIND $batch AS row"
            " CREATE (p:Person {"
            "  nconst: row.nconst,"
            "  primaryName: row.primaryName,"
            "  birthYear: row.birthYear,"
            "  deathYear: row.deathYear,"
            "  primaryProfession: row.primaryProfession,"
            "  knownForTitles: row.knownForTitles"
            " })",
            batch=data,
        )
        total += result.consume().counters.nodes_created

    print(f"  {total:,} Person nodes ({time.time() - t:.1f}s)")


def seed_titles(session: Session, con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    print("Seeding Title nodes...")
    t = time.time()
    limit_clause = f" ORDER BY b.tconst LIMIT {limit}" if limit is not None else ""
    rows = con.execute(
        "SELECT b.tconst, b.titleType, b.primaryTitle, b.originalTitle,"
        "       b.isAdult, b.startYear, b.endYear, b.runtimeMinutes, b.genres,"
        "       r.averageRating, r.numVotes"
        " FROM title_basics b"
        " LEFT JOIN title_ratings r ON b.tconst = r.tconst"
        + limit_clause
    ).fetchall()

    total = 0
    for batch in _batches(rows, BATCH_SIZE):
        data: list[dict[str, Any]] = [
            {
                "tconst": r[0],
                "titleType": _null(r[1]),
                "primaryTitle": _null(r[2]),
                "originalTitle": _null(r[3]),
                "isAdult": bool(int(r[4])) if _null(r[4]) is not None else None,
                "startYear": _int(r[5]),
                "endYear": _int(r[6]),
                "runtimeMinutes": _int(r[7]),
                "genres": _null(r[8]),
                "averageRating": _float(r[9]),
                "numVotes": _int(r[10]),
            }
            for r in batch
        ]
        result = session.run(
            "UNWIND $batch AS row"
            " CREATE (t:Title {"
            "  tconst: row.tconst,"
            "  titleType: row.titleType,"
            "  primaryTitle: row.primaryTitle,"
            "  originalTitle: row.originalTitle,"
            "  isAdult: row.isAdult,"
            "  startYear: row.startYear,"
            "  endYear: row.endYear,"
            "  runtimeMinutes: row.runtimeMinutes,"
            "  genres: row.genres,"
            "  averageRating: row.averageRating,"
            "  numVotes: row.numVotes"
            " })",
            batch=data,
        )
        total += result.consume().counters.nodes_created

    print(f"  {total:,} Title nodes ({time.time() - t:.1f}s)")


def seed_relationships(session: Session, con: duckdb.DuckDBPyConnection, limit: int | None = None) -> None:
    print("Seeding relationships...")
    t = time.time()
    if limit is not None:
        query = (
            "SELECT tconst, nconst, category, job, characters FROM title_principals"
            f" WHERE tconst IN (SELECT tconst FROM title_basics ORDER BY tconst LIMIT {limit})"
        )
    else:
        query = "SELECT tconst, nconst, category, job, characters FROM title_principals"
    rows = con.execute(query).fetchall()

    # Group by rel type — mapped categories use the substitution, others normalise to UPPER_SNAKE_CASE
    by_type: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        category = r[2]
        rel_type = CATEGORY_TO_REL_TYPE.get(category) or category.upper().replace(" ", "_")
        by_type.setdefault(rel_type, []).append(
            {
                "tconst": r[0],
                "nconst": r[1],
                "category": category,
                "job": _null(r[3]),
                "characters": _null(r[4]),
            }
        )

    total = 0
    for rel_type, rel_rows in by_type.items():
        # rel_type is either a CATEGORY_TO_REL_TYPE substitution or a normalised category name — safe to interpolate
        query = (
            "UNWIND $batch AS row"
            " MATCH (p:Person {nconst: row.nconst})"
            " MATCH (t:Title  {tconst: row.tconst})"
            f" CREATE (p)-[r:{rel_type} {{job: row.job, characters: row.characters, category: row.category}}]->(t)"
        )
        for batch in _batches(rel_rows, BATCH_SIZE):
            result = session.run(Query(cast(LiteralString, query)), batch=batch)
            total += result.consume().counters.relationships_created

    print(f"  {total:,} relationships ({time.time() - t:.1f}s)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Neo4j seed script")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Seed only the first N titles from title_basics and their related persons/relationships",
    )
    args = parser.parse_args()
    limit: int | None = args.limit

    t_start = time.time()

    uri, user, password = load_env()
    driver = get_driver(uri, user, password)

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    with driver.session() as session:  # type: ignore[misc]
        wipe_database(session)
        create_schema(session)
        seed_persons(session, con, limit=limit)
        seed_titles(session, con, limit=limit)
        seed_relationships(session, con, limit=limit)

    con.close()
    driver.close()

    print(f"\nDone in {time.time() - t_start:.1f}s")


if __name__ == "__main__":
    main()
