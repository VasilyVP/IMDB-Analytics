from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from app.core.config import Settings

_ENV_TEST_FILE = str(Path(__file__).resolve().parents[2] / ".env.test")


class SettingsTests(unittest.TestCase):
    def test_settings_loads_from_env_test_file(self) -> None:
        try:
            settings = Settings(_env_file=_ENV_TEST_FILE)  # type: ignore[call-arg]
        except ValidationError as exc:
            self.fail(f"Settings should load from .env.test but raised: {exc}")

        self.assertEqual(settings.NEO4J_URI, "bolt://localhost:7687")
        self.assertEqual(settings.NEO4J_USER, "neo4j")
        self.assertEqual(settings.NEO4J_PASSWORD, "password")
        self.assertEqual(settings.CHROMA_HOST, "localhost")
        self.assertEqual(settings.CHROMA_PORT, 8001)
        self.assertEqual(settings.CHROMA_COLLECTION_TITLES, "titles")
        self.assertEqual(settings.CHROMA_COLLECTION_PERSONS, "persons")
        self.assertEqual(settings.TEXT_GENERATION_MODEL, "llama3.2:3b")
        self.assertEqual(
            settings.OPENAI_BASE_URL,
            "http://localhost:11434/v1/chat/completions",
        )
        self.assertEqual(settings.OPENAI_API_KEY, "any-api-key")
        self.assertEqual(settings.LLM_MAX_RETRIES, 3)
        self.assertEqual(settings.HUMAN_MAX_TOKENS, 200)
        self.assertEqual(settings.EMBEDDING_MAX_TOKENS, 250)

    def test_settings_ignores_legacy_extra_variables(self) -> None:
        try:
            Settings(_env_file=_ENV_TEST_FILE)  # type: ignore[call-arg]
        except ValidationError as exc:
            self.fail(f"Legacy extra vars in .env.test should be ignored but raised: {exc}")

    def test_settings_requires_chroma_and_llm_variables(self) -> None:
        with patch.dict(
            os.environ,
            {
                "CHROMA_HOST": "",
                "CHROMA_PORT": "",
                "CHROMA_COLLECTION_TITLES": "",
                "CHROMA_COLLECTION_PERSONS": "",
                "TEXT_GENERATION_MODEL": "",
                "OPENAI_BASE_URL": "",
                "OPENAI_API_KEY": "",
                "LLM_MAX_RETRIES": "",
                "HUMAN_MAX_TOKENS": "",
                "EMBEDDING_MAX_TOKENS": "",
            },
        ):
            with self.assertRaises(ValidationError):
                Settings(
                    NEO4J_URI="bolt://localhost:7687",
                    NEO4J_USER="neo4j",
                    NEO4J_PASSWORD="password",
                )  # type: ignore[call-arg]


if __name__ == "__main__":
    unittest.main()
