from __future__ import annotations

import unittest
from unittest.mock import ANY, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.endpoints.query import router as query_router
from app.core.database import get_duckdb
from app.repositories import search_repository
from app.schemas.search import SearchQueryParams, SearchResponse, SearchResultItem
from app.services import search_service


class _RecordingDuckDBConnection:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_params: list[object] = []

    def execute(self, sql: str, params: list[object]) -> "_RecordingDuckDBConnection":
        self.last_sql = " ".join(sql.split())
        self.last_params = params
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class SearchServiceTests(unittest.TestCase):
    def test_search_validates_trimmed_query_length(self) -> None:
        with self.assertRaisesRegex(Exception, "at least 3"):
            SearchQueryParams(q="  ab  ")

    def test_search_rejects_whitespace_only_query(self) -> None:
        with self.assertRaisesRegex(Exception, "at least 3"):
            SearchQueryParams(q="   ")

    def test_search_validates_rating_range(self) -> None:
        with self.assertRaisesRegex(Exception, "minRating"):
            SearchQueryParams(q="shaw", minRating=9.5, maxRating=8.0)

    def test_search_validates_year_range(self) -> None:
        with self.assertRaisesRegex(Exception, "startYearFrom"):
            SearchQueryParams(q="shaw", startYearFrom=2020, startYearTo=2010)

    def test_search_rejects_limit_above_50(self) -> None:
        with self.assertRaisesRegex(Exception, "less than or equal to 50"):
            SearchQueryParams(q="nolan", limit=500)

    @patch("app.services.search_service.search_repository.search", return_value=[])
    def test_search_uses_limit_from_params(self, mocked_search: object) -> None:
        params = SearchQueryParams(q="nolan", limit=50)
        search_service.search(object(), params)

        mocked_search.assert_called_once()
        self.assertEqual(mocked_search.call_args.kwargs["limit"], 50)

    @patch(
        "app.services.search_service.search_repository.search",
        return_value=[
            search_repository.SearchRow(
                id="tt0111161",
                result="The Shawshank Redemption",
                title_type="movie",
            ),
            search_repository.SearchRow(
                id="nm0000209",
                result="Tim Robbins",
                title_type="_",
            ),
            search_repository.SearchRow(
                id="tt0137523",
                result="Fight Club",
                title_type="short",
            ),
        ],
    )
    def test_search_merges_results_with_expected_shape(self, _mocked_search: object) -> None:
        payload = search_service.search(object(), SearchQueryParams(q="shaw", limit=10))

        self.assertEqual(
            payload,
            SearchResponse(
                results=[
                    SearchResultItem(
                        id="tt0111161",
                        primaryTitle="The Shawshank Redemption",
                    ),
                    SearchResultItem(id="nm0000209", name="Tim Robbins"),
                    SearchResultItem(
                        id="tt0137523",
                        primaryTitle="Fight Club",
                    ),
                ]
            ),
        )

    @patch("app.services.search_service.search_repository.search", return_value=[])
    def test_search_normalizes_filter_tokens_before_repository_calls(
        self,
        mocked_search: object,
    ) -> None:
        search_service.search(
            object(),
            SearchQueryParams(
                q="dark",
                genre=" Drama ",
                titleType=" tvSeries ",
            ),
        )

        self.assertEqual(mocked_search.call_args.kwargs["genre"], "drama")
        self.assertEqual(mocked_search.call_args.kwargs["title_type"], "tvseries")

    def test_search_enforces_q_max_length_20(self) -> None:
        with self.assertRaisesRegex(Exception, "at most 20"):
            SearchQueryParams(q="a" * 21)


class SearchRepositoryTests(unittest.TestCase):
    def test_search_uses_top_rated_view_when_requested(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="shaw",
            limit=10,
            source_relation="top_rated_titles",
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("FROM top_rated_titles", conn.last_sql)

    def test_search_uses_combined_view_when_both_flags_true(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="nolan",
            limit=10,
            source_relation="top_rated_popular_titles",
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("FROM top_rated_popular_titles", conn.last_sql)

    def test_search_passes_wildcard_pattern_as_is(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="%leo%%caprio%",
            limit=10,
            source_relation=None,
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("%leo%%caprio%", conn.last_params)

    def test_search_wraps_plain_query_in_wildcards(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="nolan",
            limit=10,
            source_relation=None,
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("%nolan%", conn.last_params)

    def test_search_joins_words_with_wildcards(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="leo caprio",
            limit=10,
            source_relation=None,
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("%leo%caprio%", conn.last_params)

    def test_search_persons_include_exists_when_filters_active(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="leo",
            limit=10,
            source_relation=None,
            min_rating=None,
            max_rating=None,
            start_year_from=2000,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertIn("EXISTS", conn.last_sql)
        self.assertIn("title_principals", conn.last_sql)

    def test_search_persons_no_exists_when_no_filters(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="leo",
            limit=10,
            source_relation=None,
            min_rating=None,
            max_rating=None,
            start_year_from=None,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        self.assertNotIn("title_principals", conn.last_sql)

    def test_search_person_filters_use_same_params_as_title_filters(self) -> None:
        conn = _RecordingDuckDBConnection()

        search_repository.search(
            conn,
            query="leo",
            limit=10,
            source_relation=None,
            min_rating=7.5,
            max_rating=None,
            start_year_from=2000,
            start_year_to=None,
            genre=None,
            title_type=None,
        )

        # filter_params appear twice: once for person EXISTS, once for title WHERE
        params = conn.last_params
        rating_indices = [i for i, p in enumerate(params) if p == 7.5]
        year_indices = [i for i, p in enumerate(params) if p == 2000]
        self.assertEqual(len(rating_indices), 2)
        self.assertEqual(len(year_indices), 2)


class SearchEndpointTests(unittest.TestCase):
    def test_search_endpoint_forwards_query_params(self) -> None:
        app = FastAPI()
        app.include_router(query_router, prefix="/api/query", tags=["query"])
        app.dependency_overrides[get_duckdb] = lambda: object()

        expected = SearchResponse(
            results=[
                SearchResultItem(
                    id="tt0111161",
                    primaryTitle="The Shawshank Redemption",
                ),
                SearchResultItem(id="nm0000209", name="Tim Robbins"),
            ]
        )

        with patch(
            "app.api.endpoints.query.search_service.search",
            return_value=expected,
        ) as mocked_search:
            with TestClient(app) as client:
                response = client.get(
                    "/api/query/search"
                    "?q=shaw"
                    "&limit=50"
                    "&topRated=true"
                    "&mostPopular=false"
                    "&minRating=7.5"
                    "&maxRating=9.9"
                    "&startYearFrom=1990"
                    "&startYearTo=2020"
                    "&genre=Drama"
                    "&titleType=movie"
                )

        self.assertEqual(response.status_code, 200)
        mocked_search.assert_called_once_with(
            ANY,
            SearchQueryParams(
                q="shaw",
                limit=50,
                topRated=True,
                mostPopular=False,
                minRating=7.5,
                maxRating=9.9,
                startYearFrom=1990,
                startYearTo=2020,
                genre="Drama",
                titleType="movie",
            ),
        )
        self.assertEqual(
            response.json(),
            {
                "results": [
                    {
                        "id": "tt0111161",
                        "primaryTitle": "The Shawshank Redemption",
                    },
                    {
                        "id": "nm0000209",
                        "name": "Tim Robbins",
                    },
                ]
            },
        )

    def test_search_endpoint_requires_q(self) -> None:
        app = FastAPI()
        app.include_router(query_router, prefix="/api/query", tags=["query"])
        app.dependency_overrides[get_duckdb] = lambda: object()

        with TestClient(app) as client:
            response = client.get("/api/query/search")

        self.assertEqual(response.status_code, 422)

    def test_search_endpoint_validates_range_with_schema(self) -> None:
        app = FastAPI()
        app.include_router(query_router, prefix="/api/query", tags=["query"])
        app.dependency_overrides[get_duckdb] = lambda: object()

        with TestClient(app) as client:
            response = client.get("/api/query/search?q=shaw&minRating=9.0&maxRating=8.0")

        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
