from __future__ import annotations

import unittest

from pydantic import ValidationError

from app.core.config import Settings


class SettingsTests(unittest.TestCase):
    def test_settings_ignores_extra_inputs(self) -> None:
        try:
            settings = Settings(
                NEO4J_URI="bolt://localhost:7687",
                NEO4J_USER="neo4j",
                NEO4J_PASSWORD="password",
                HF_TOKEN="token",
                VLLM_MODEL="model",
            )
        except ValidationError as exc:
            self.fail(f"Settings should ignore extra inputs but raised: {exc}")

        self.assertEqual(settings.NEO4J_URI, "bolt://localhost:7687")
        self.assertEqual(settings.NEO4J_USER, "neo4j")
        self.assertEqual(settings.NEO4J_PASSWORD, "password")


if __name__ == "__main__":
    unittest.main()
