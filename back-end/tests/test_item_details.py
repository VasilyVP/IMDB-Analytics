from __future__ import annotations

from typing import cast
import unittest
from unittest.mock import ANY, patch

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.endpoints.query import router as query_router
from app.core.database import get_duckdb
from app.core.limiter import limiter
from app.schemas.item_details import ItemDetailsParams, ItemDetailsResponse
from app.services import item_details_service


class ItemDetailsParamsTests(unittest.TestCase):
    def test_requires_exactly_one_identifier(self) -> None:
        with self.assertRaisesRegex(Exception, "Exactly one"):
            ItemDetailsParams()

        with self.assertRaisesRegex(Exception, "Exactly one"):
            ItemDetailsParams(titleId="tt0111161", nameId="nm0000209")

    def test_rejects_malformed_title_id(self) -> None:
        with self.assertRaisesRegex(Exception, "titleId"):
            ItemDetailsParams(titleId="bad-id")

    def test_rejects_malformed_name_id(self) -> None:
        with self.assertRaisesRegex(Exception, "nameId"):
            ItemDetailsParams(nameId="bad-id")

    def test_accepts_title_id(self) -> None:
        params = ItemDetailsParams(titleId="tt0111161")
        self.assertEqual(params.title_id, "tt0111161")
        self.assertIsNone(params.name_id)

    def test_accepts_name_id(self) -> None:
        params = ItemDetailsParams(nameId="nm0000209")
        self.assertEqual(params.name_id, "nm0000209")
        self.assertIsNone(params.title_id)


class ItemDetailsServiceTests(unittest.TestCase):
    def test_returns_cached_title_description_when_present(self) -> None:
        with patch.object(
            item_details_service.item_details_repository,
            "get_title_description",
            return_value="Cached title description",
        ) as mocked_get_title_description:
            result = item_details_service.get_item_details(
                cast(duckdb.DuckDBPyConnection, object()),
                ItemDetailsParams(titleId="tt0111161"),
            )

        mocked_get_title_description.assert_called_once_with("tt0111161")
        self.assertEqual(
            result,
            ItemDetailsResponse(
                id="tt0111161",
                entityType="title",
                description="Cached title description",
            ),
        )

    def test_fallback_generates_and_persists_title_description(self) -> None:
        with (
            patch.object(
                item_details_service.item_details_repository,
                "get_title_description",
                return_value=None,
            ),
            patch.object(
                item_details_service.item_details_repository,
                "fetch_title_prompt_input",
                return_value=item_details_service.item_details_repository.TitlePromptInput(
                    title_id="tt0111161",
                    title="The Shawshank Redemption",
                    start_year=1994,
                ),
            ),
            patch.object(
                item_details_service.llm_service,
                "request_completion",
                side_effect=["Generated human description", "Generated embedding"],
            ) as mocked_generate,
            patch.object(
                item_details_service.item_details_repository,
                "upsert_title_description",
            ) as mocked_upsert,
        ):
            result = item_details_service.get_item_details(
                cast(duckdb.DuckDBPyConnection, object()),
                ItemDetailsParams(titleId="tt0111161"),
            )

        self.assertEqual(mocked_generate.call_count, 2)
        mocked_upsert.assert_called_once_with(
            title_id="tt0111161",
            title="The Shawshank Redemption",
            start_year=1994,
            human_description="Generated human description",
            embedding_description="Generated embedding",
        )
        self.assertEqual(result.entityType, "title")
        self.assertEqual(result.description, "Generated human description")

    def test_raises_not_found_when_entity_missing_for_fallback(self) -> None:
        with (
            patch.object(
                item_details_service.item_details_repository,
                "get_person_description",
                return_value=None,
            ),
            patch.object(
                item_details_service.item_details_repository,
                "fetch_person_prompt_input",
                return_value=None,
            ),
        ):
            with self.assertRaises(item_details_service.ItemDetailsNotFoundError):
                item_details_service.get_item_details(
                    cast(duckdb.DuckDBPyConnection, object()),
                    ItemDetailsParams(nameId="nm0000209"),
                )

    def test_raises_unavailable_on_generation_failure(self) -> None:
        with (
            patch.object(
                item_details_service.item_details_repository,
                "get_title_description",
                return_value=None,
            ),
            patch.object(
                item_details_service.item_details_repository,
                "fetch_title_prompt_input",
                return_value=item_details_service.item_details_repository.TitlePromptInput(
                    title_id="tt0111161",
                    title="The Shawshank Redemption",
                    start_year=1994,
                ),
            ),
            patch.object(
                item_details_service.llm_service,
                "request_completion",
                side_effect=RuntimeError("llm failed"),
            ),
        ):
            with self.assertRaises(item_details_service.ItemDetailsUnavailableError):
                item_details_service.get_item_details(
                    cast(duckdb.DuckDBPyConnection, object()),
                    ItemDetailsParams(titleId="tt0111161"),
                )


class ItemDetailsEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        limiter.reset()

    def _build_app(self) -> FastAPI:
        app = FastAPI()
        app.include_router(query_router, prefix="/api/query", tags=["query"])
        app.dependency_overrides[get_duckdb] = lambda: object()
        return app

    def test_item_details_endpoint_forwards_title_params(self) -> None:
        app = self._build_app()

        expected = ItemDetailsResponse(
            id="tt0111161",
            entityType="title",
            description="A banker convicted of murdering his wife...",
        )

        with patch(
            "app.api.endpoints.query.item_details_service.get_item_details",
            return_value=expected,
        ) as mocked_get_item_details:
            with TestClient(app) as client:
                response = client.get("/api/query/item-details?titleId=tt0111161")

        self.assertEqual(response.status_code, 200)
        mocked_get_item_details.assert_called_once_with(
            ANY,
            ItemDetailsParams(titleId="tt0111161"),
        )
        self.assertEqual(
            response.json(),
            {
                "id": "tt0111161",
                "entityType": "title",
                "description": "A banker convicted of murdering his wife...",
            },
        )

    def test_item_details_endpoint_rejects_invalid_query_shape(self) -> None:
        app = self._build_app()

        with TestClient(app) as client:
            response = client.get("/api/query/item-details")

        self.assertEqual(response.status_code, 422)

    def test_item_details_endpoint_maps_not_found_to_404(self) -> None:
        app = self._build_app()

        with patch(
            "app.api.endpoints.query.item_details_service.get_item_details",
            side_effect=item_details_service.ItemDetailsNotFoundError,
        ):
            with TestClient(app) as client:
                response = client.get("/api/query/item-details?nameId=nm0000209")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Description not found"})

    def test_item_details_endpoint_maps_unavailable_to_503(self) -> None:
        app = self._build_app()

        with patch(
            "app.api.endpoints.query.item_details_service.get_item_details",
            side_effect=item_details_service.ItemDetailsUnavailableError,
        ):
            with TestClient(app) as client:
                response = client.get("/api/query/item-details?titleId=tt0111161")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(),
            {"detail": "Description service is temporarily unavailable"},
        )


if __name__ == "__main__":
    unittest.main()
