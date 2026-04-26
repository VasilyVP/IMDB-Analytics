from __future__ import annotations

from typing import Any, cast
import unittest
from unittest.mock import ANY, patch

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.endpoints.query import router as query_router
from app.core.database import get_duckdb
from app.core.limiter import limiter
from app.repositories import human_to_llm_repository
from app.schemas.human_to_llm_query import (
    HumanToLlmParsedFields,
    HumanToLlmQueryRequest,
    HumanToLlmQueryResponse,
    HumanToLlmResultItem,
)
from app.services import human_to_llm_query_service


class _RecordingDuckDBConnection:
    def __init__(self, result_sets: list[list[tuple[object, ...]]]) -> None:
        self._result_sets = result_sets
        self.executed: list[tuple[str, list[object]]] = []

    def execute(self, sql: str, params: list[object]) -> _RecordingDuckDBConnection:
        self.executed.append((" ".join(sql.split()), list(params)))
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        if not self._result_sets:
            return []
        return self._result_sets.pop(0)


class HumanToLlmQuerySchemaTests(unittest.TestCase):
    def test_request_trims_query_and_uses_default_limit(self) -> None:
        payload = HumanToLlmQueryRequest(query="  who is Christopher Nolan  ")

        self.assertEqual(payload.query, "who is Christopher Nolan")
        self.assertEqual(payload.limit, 10)

    def test_request_rejects_whitespace_only_query(self) -> None:
        with self.assertRaisesRegex(Exception, "must not be empty"):
            HumanToLlmQueryRequest(query="   ")

    def test_request_rejects_limit_above_50(self) -> None:
        with self.assertRaisesRegex(Exception, "less than or equal to 50"):
            HumanToLlmQueryRequest(query="some query", limit=51)


class HumanToLlmRepositoryTests(unittest.TestCase):
    def test_find_person_requires_non_null_birth_year(self) -> None:
        conn = _RecordingDuckDBConnection(
            result_sets=[[("nm0000184", "Christopher Nolan", 1970)]],
        )

        human_to_llm_repository.lookup_persons(
            cast(duckdb.DuckDBPyConnection, conn),
            name="Christopher Nolan",
            limit=10,
            role=None,
        )

        self.assertEqual(len(conn.executed), 1)
        self.assertIn("nu.birthYear IS NOT NULL", conn.executed[0][0])

    def test_find_person_prefers_exact_match(self) -> None:
        conn = _RecordingDuckDBConnection(
            result_sets=[[("nm0000184", "Christopher Nolan", 1970)]],
        )

        rows = human_to_llm_repository.lookup_persons(
            cast(duckdb.DuckDBPyConnection, conn),
            name="Christopher Nolan",
            limit=10,
            role="director",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, "nm0000184")
        self.assertEqual(rows[0].birth_year, 1970)
        self.assertIsNone(rows[0].start_year)
        self.assertEqual(len(conn.executed), 1)
        self.assertIn("lower(nu.primaryName) = lower(?)", conn.executed[0][0])

    def test_find_person_falls_back_to_prefix_match(self) -> None:
        conn = _RecordingDuckDBConnection(
            result_sets=[[], [("nm0000184", "Christopher Nolan", 1970)]],
        )

        rows = human_to_llm_repository.lookup_persons(
            cast(duckdb.DuckDBPyConnection, conn),
            name="Christopher",
            limit=10,
            role=None,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(conn.executed), 2)
        self.assertIn("lower(nu.primaryName) LIKE lower(?)", conn.executed[1][0])
        self.assertEqual(conn.executed[1][1][0], "Christopher%")

    def test_find_title_uses_single_ilike_pass_with_type_filter_and_ordering(self) -> None:
        conn = _RecordingDuckDBConnection(
            result_sets=[[("tt0816692", "Interstellar", 2014)]],
        )

        rows = human_to_llm_repository.lookup_titles(
            cast(duckdb.DuckDBPyConnection, conn),
            title="Interstellar",
            limit=5,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].id, "tt0816692")
        self.assertEqual(rows[0].label, "Interstellar")
        self.assertIsNone(rows[0].birth_year)
        self.assertEqual(rows[0].start_year, 2014)
        self.assertEqual(len(conn.executed), 1)
        sql, params = conn.executed[0]
        self.assertIn("ILIKE ?", sql)
        self.assertEqual(params[0], "%Interstellar%")
        self.assertIn("titleType IN ('movie', 'tvSeries')", sql)
        self.assertIn("tb.startYear IS NOT NULL", sql)
        self.assertIn("ORDER BY tb.titleType ASC, tb.startYear DESC", sql)
        self.assertEqual(params[1], 5)


class HumanToLlmQueryServiceTests(unittest.TestCase):
    @patch("app.services.human_to_llm_query_service._classify_query")
    @patch("app.services.human_to_llm_query_service.human_to_llm_repository.search_similarity")
    @patch("app.services.human_to_llm_query_service.human_to_llm_repository.lookup_persons")
    def test_person_search_with_name_and_role_uses_person_lookup(
        self,
        mocked_lookup_persons: Any,
        mocked_similarity: Any,
        mocked_classify: Any,
    ) -> None:
        mocked_classify.return_value = human_to_llm_query_service.ClassifiedQuery(
            type="person_search",
            parsed=HumanToLlmParsedFields(
                role="actor",
                name="Keanu Reeves",
                title=None,
                details="actor who played Neo in Matrix",
            ),
        )
        mocked_lookup_persons.return_value = [
            human_to_llm_repository.DuckDBLookupRow(
                id="nm0000206",
                label="Keanu Reeves",
                entity_type="person",
                birth_year=1964,
                start_year=None,
            )
        ]

        response = human_to_llm_query_service.human_to_llm_query(
            cast(duckdb.DuckDBPyConnection, object()),
            HumanToLlmQueryRequest(
                query="actor who played Neo in Matrix",
                limit=5,
            ),
        )

        mocked_lookup_persons.assert_called_once_with(
            ANY,
            name="Keanu Reeves",
            limit=5,
            role="actor",
        )
        mocked_similarity.assert_not_called()
        self.assertEqual(response.type, "person_search")
        self.assertEqual(response.results[0].id, "nm0000206")

    @patch("app.services.human_to_llm_query_service._classify_query")
    @patch("app.services.human_to_llm_query_service.human_to_llm_repository.search_similarity")
    def test_recommendation_uses_original_query_when_details_missing(
        self,
        mocked_similarity: Any,
        mocked_classify: Any,
    ) -> None:
        mocked_classify.return_value = human_to_llm_query_service.ClassifiedQuery(
            type="recommendation",
            parsed=HumanToLlmParsedFields(
                role="actor",
                name="Bruce Willis",
                title=None,
                details=None,
            ),
        )
        mocked_similarity.return_value = [
            human_to_llm_repository.SimilarityRow(
                id="nm0000112",
                label="Nicolas Cage",
                entity_type="person",
                birth_year=1964,
                start_year=None,
                score=0.19,
            )
        ]

        response = human_to_llm_query_service.human_to_llm_query(
            cast(duckdb.DuckDBPyConnection, object()),
            HumanToLlmQueryRequest(
                query="recommend me actors similar to bruce willis",
                limit=5,
            ),
        )

        self.assertEqual(response.type, "recommendation")
        self.assertEqual(response.parsed.name, "Bruce Willis")
        mocked_similarity.assert_called_once_with(
            query_text="recommend me actors similar to bruce willis",
            limit=5,
            entity_type="person",
            category="actor",
        )


class HumanToLlmQueryEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        limiter.reset()

    def _build_app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(query_router, prefix="/api/query", tags=["query"])
        app.dependency_overrides[get_duckdb] = lambda: object()
        return app

    def test_endpoint_forwards_body_and_returns_response(self) -> None:
        app = self._build_app()
        expected = HumanToLlmQueryResponse(
            type="person",
            parsed=HumanToLlmParsedFields(
                role="director",
                name="Christopher Nolan",
                title=None,
                details=None,
            ),
            results=[
                HumanToLlmResultItem(
                    id="nm0634240",
                    label="Christopher Nolan",
                    entityType="person",
                    birthYear=1970,
                    startYear=None,
                    score=None,
                )
            ],
        )

        with patch(
            "app.api.endpoints.query.human_to_llm_query_service.human_to_llm_query",
            return_value=expected,
        ) as mocked_service:
            with TestClient(app) as client:
                response = client.post(
                    "/api/query/human-to-llm",
                    json={"query": "  who is Christopher Nolan  ", "limit": 10},
                )

        self.assertEqual(response.status_code, 200)
        mocked_service.assert_called_once_with(
            ANY,
            HumanToLlmQueryRequest(query="who is Christopher Nolan", limit=10),
        )
        self.assertEqual(response.json()["type"], "person")
        self.assertEqual(response.json()["results"][0]["id"], "nm0634240")
        self.assertEqual(response.json()["results"][0]["birthYear"], 1970)
        self.assertIsNone(response.json()["results"][0]["startYear"])

    def test_endpoint_maps_parse_failures_to_502(self) -> None:
        app = self._build_app()

        with patch(
            "app.api.endpoints.query.human_to_llm_query_service.human_to_llm_query",
            side_effect=human_to_llm_query_service.HumanToLlmQueryParseError,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/query/human-to-llm",
                    json={"query": "who is Christopher Nolan", "limit": 10},
                )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json(),
            {"detail": "LLM response could not be parsed"},
        )

    def test_endpoint_maps_availability_failures_to_503(self) -> None:
        app = self._build_app()

        with patch(
            "app.api.endpoints.query.human_to_llm_query_service.human_to_llm_query",
            side_effect=human_to_llm_query_service.HumanToLlmQueryUnavailableError,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/query/human-to-llm",
                    json={"query": "who is Christopher Nolan", "limit": 10},
                )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(),
            {"detail": "Search backend is temporarily unavailable"},
        )


if __name__ == "__main__":
    unittest.main()
