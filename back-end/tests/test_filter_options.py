from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import ANY, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.database import get_duckdb
from app.api.endpoints.titles import router as titles_router
from app.schemas.filter_options import (
    FilterOptionsResponse,
    NumericRangeFloat,
    NumericRangeInt,
    TitleTypeOption,
)
from app.services import filter_service


class _FakeDuckDBResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        if not self._rows:
            return None
        return self._rows[0]


class _FakeDuckDBConnection:
    def __init__(self) -> None:
        self.query_count = 0
        self.executed_sql: list[str] = []

    def execute(self, sql: str) -> _FakeDuckDBResult:
        self.query_count += 1
        normalized = " ".join(sql.split())
        self.executed_sql.append(normalized)
        normalized_lower = normalized.lower()

        source = "base"
        if "from top_rated_popular_titles" in normalized_lower:
            source = "top_rated_popular"
        elif "from top_rated_titles" in normalized_lower:
            source = "top_rated"
        elif "from most_popular_titles" in normalized_lower:
            source = "most_popular"

        if "unnest(string_split(tb.genres, ','))" in normalized:
            if source == "top_rated":
                return _FakeDuckDBResult([("Drama",), ("Thriller",)])
            if source == "most_popular":
                return _FakeDuckDBResult([("Action",), ("Adventure",)])
            if source == "top_rated_popular":
                return _FakeDuckDBResult([("Crime",), ("Drama",)])
            return _FakeDuckDBResult([("Action",), ("Comedy",)])

        if "SELECT DISTINCT titleType" in normalized:
            if source == "top_rated":
                return _FakeDuckDBResult([("movie",), ("tvSeries",)])
            if source == "most_popular":
                return _FakeDuckDBResult([("movie",), ("tvMiniSeries",)])
            if source == "top_rated_popular":
                return _FakeDuckDBResult([("movie",)])
            return _FakeDuckDBResult([("movie",), ("tvSeries",), ("videoGame",)])

        if "MIN(CAST(startYear AS INTEGER))" in normalized:
            if source == "top_rated":
                return _FakeDuckDBResult([(1950, 2022)])
            if source == "most_popular":
                return _FakeDuckDBResult([(1980, 2024)])
            if source == "top_rated_popular":
                return _FakeDuckDBResult([(1990, 2021)])
            return _FakeDuckDBResult([(1894, 2025)])

        if "MIN(averageRating)" in normalized:
            if source == "top_rated":
                return _FakeDuckDBResult([(8.0, 9.9)])
            if source == "most_popular":
                return _FakeDuckDBResult([(5.5, 9.5)])
            if source == "top_rated_popular":
                return _FakeDuckDBResult([(8.2, 9.8)])
            return _FakeDuckDBResult([(1.2, 9.9)])

        raise AssertionError(f"Unexpected SQL: {sql}")


class _SparseFakeDuckDBConnection:
    def execute(self, sql: str) -> _FakeDuckDBResult:
        normalized = " ".join(sql.split())

        if "unnest(string_split(tb.genres, ','))" in normalized:
            return _FakeDuckDBResult([])

        if "SELECT DISTINCT titleType" in normalized:
            return _FakeDuckDBResult([])

        if "MIN(CAST(startYear AS INTEGER))" in normalized:
            return _FakeDuckDBResult([(None, None)])

        if "MIN(averageRating)" in normalized:
            return _FakeDuckDBResult([(None, None)])

        raise AssertionError(f"Unexpected SQL: {sql}")


class FilterOptionsServiceTests(unittest.TestCase):
    def tearDown(self) -> None:
        filter_service.get_filter_options.cache_clear()

    def test_get_filter_options_returns_expected_payload_shape(self) -> None:
        conn = _FakeDuckDBConnection()

        payload = filter_service.get_filter_options(conn)

        self.assertEqual(payload.genres, ["Action", "Comedy"])
        self.assertEqual(
            payload.titleTypes,
            [
                TitleTypeOption(value="movie", label="Movie"),
                TitleTypeOption(value="tvSeries", label="TV Series"),
                TitleTypeOption(value="videoGame", label="Video Game"),
            ],
        )
        self.assertEqual(payload.yearRange, NumericRangeInt(min=1894, max=2025))
        self.assertEqual(payload.ratingRange, NumericRangeFloat(min=1.2, max=9.9))

    def test_get_filter_options_uses_cache_after_first_call(self) -> None:
        conn = _FakeDuckDBConnection()

        first = filter_service.get_filter_options(conn)
        second = filter_service.get_filter_options(conn)

        self.assertEqual(first, second)
        self.assertEqual(conn.query_count, 4)

    def test_get_filter_options_cache_is_split_by_flag_combination(self) -> None:
        conn = _FakeDuckDBConnection()

        first = filter_service.get_filter_options(conn)
        second = filter_service.get_filter_options(conn)
        third = filter_service.get_filter_options(conn, top_rated=True)

        self.assertEqual(first, second)
        self.assertNotEqual(first, third)
        self.assertEqual(conn.query_count, 8)

    def test_get_filter_options_top_rated_uses_top_rated_view(self) -> None:
        conn = _FakeDuckDBConnection()

        payload = filter_service.get_filter_options(conn, top_rated=True)

        self.assertEqual(payload.genres, ["Drama", "Thriller"])
        self.assertEqual(payload.yearRange, NumericRangeInt(min=1950, max=2022))
        self.assertEqual(payload.ratingRange, NumericRangeFloat(min=8.0, max=9.9))
        self.assertTrue(all("from top_rated_titles" in query.lower() for query in conn.executed_sql))

    def test_get_filter_options_most_popular_uses_most_popular_view(self) -> None:
        conn = _FakeDuckDBConnection()

        payload = filter_service.get_filter_options(conn, most_popular=True)

        self.assertEqual(payload.genres, ["Action", "Adventure"])
        self.assertEqual(payload.yearRange, NumericRangeInt(min=1980, max=2024))
        self.assertEqual(payload.ratingRange, NumericRangeFloat(min=5.5, max=9.5))
        self.assertTrue(all("from most_popular_titles" in query.lower() for query in conn.executed_sql))

    def test_get_filter_options_with_both_flags_uses_combined_view(self) -> None:
        conn = _FakeDuckDBConnection()

        payload = filter_service.get_filter_options(
            conn,
            top_rated=True,
            most_popular=True,
        )

        self.assertEqual(payload.genres, ["Crime", "Drama"])
        self.assertEqual(payload.yearRange, NumericRangeInt(min=1990, max=2021))
        self.assertEqual(payload.ratingRange, NumericRangeFloat(min=8.2, max=9.8))
        self.assertTrue(all("from top_rated_popular_titles" in query.lower() for query in conn.executed_sql))

    def test_get_filter_options_handles_sparse_data(self) -> None:
        conn = _SparseFakeDuckDBConnection()

        payload = filter_service.get_filter_options(conn)

        self.assertEqual(payload, FilterOptionsResponse(
            genres=[],
            titleTypes=[],
            yearRange=NumericRangeInt(min=None, max=None),
            ratingRange=NumericRangeFloat(min=None, max=None),
        ))


class FilterOptionsEndpointTests(unittest.TestCase):
    def test_filters_endpoint_returns_service_payload(self) -> None:
        app = FastAPI()
        app.include_router(titles_router, prefix="/api/titles", tags=["titles"])

        app.dependency_overrides[get_duckdb] = lambda: object()

        expected = FilterOptionsResponse(
            genres=["Action"],
            titleTypes=[TitleTypeOption(value="movie", label="Movie")],
            yearRange=NumericRangeInt(min=1900, max=2024),
            ratingRange=NumericRangeFloat(min=1.0, max=10.0),
        )

        with patch("app.api.endpoints.titles.filter_service.get_filter_options", return_value=expected) as mocked_get_filter_options:
            with TestClient(app) as client:
                response = client.get("/api/titles/filters")

        self.assertEqual(response.status_code, 200)
        mocked_get_filter_options.assert_called_once_with(ANY, top_rated=False, most_popular=False)
        self.assertEqual(response.json(), {
            "genres": ["Action"],
            "titleTypes": [{"value": "movie", "label": "Movie"}],
            "yearRange": {"min": 1900, "max": 2024},
            "ratingRange": {"min": 1.0, "max": 10.0},
        })

    def test_filters_endpoint_forwards_query_flags(self) -> None:
        app = FastAPI()
        app.include_router(titles_router, prefix="/api/titles", tags=["titles"])

        app.dependency_overrides[get_duckdb] = lambda: object()

        expected = FilterOptionsResponse(
            genres=["Drama"],
            titleTypes=[TitleTypeOption(value="movie", label="Movie")],
            yearRange=NumericRangeInt(min=1950, max=2022),
            ratingRange=NumericRangeFloat(min=8.0, max=9.9),
        )

        with patch("app.api.endpoints.titles.filter_service.get_filter_options", return_value=expected) as mocked_get_filter_options:
            with TestClient(app) as client:
                response = client.get("/api/titles/filters?topRated=true&mostPopular=true")

        self.assertEqual(response.status_code, 200)
        mocked_get_filter_options.assert_called_once_with(ANY, top_rated=True, most_popular=True)


if __name__ == "__main__":
    unittest.main()
