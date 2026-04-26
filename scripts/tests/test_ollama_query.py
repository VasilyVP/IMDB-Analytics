from __future__ import annotations

import io
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

_REPO_ROOT = Path(__file__).resolve().parents[2]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.llm_query as llm_query  # noqa: E402


class OllamaQueryTests(unittest.TestCase):
    def test_build_prompt_embeds_user_input(self) -> None:
        prompt = llm_query.build_prompt('movies like Inception')

        self.assertIn('Input: "movies like Inception"', prompt)
        self.assertIn('Output:', prompt)
        self.assertIn('type', prompt)

    def test_send_query_to_openai_uses_structured_outputs(self) -> None:
        expected = llm_query.QueryResponse(
            type="film",
            details=llm_query.QueryDetails(
                role=None,
                name=None,
                title="Inception",
                details=None,
            ),
        )

        parse_mock = Mock(return_value=SimpleNamespace(output_parsed=expected))
        responses_mock = Mock(parse=parse_mock)
        fake_client = Mock(responses=responses_mock)

        with patch("scripts.llm_query.OpenAI", return_value=fake_client) as openai_ctor:
            actual = llm_query.send_query_to_openai(
                prompt="My prompt",
                model="llama3.2:3b",
                host="http://localhost:11434",
                timeout_seconds=7.5,
            )

        self.assertEqual(actual, expected)
        openai_ctor.assert_called_once_with(
            base_url="http://localhost:11434/v1",
            api_key="ollama",
            timeout=7.5,
        )
        parse_mock.assert_called_once()
        kwargs = parse_mock.call_args.kwargs
        self.assertEqual(kwargs["model"], "llama3.2:3b")
        self.assertEqual(kwargs["input"], "My prompt")
        self.assertEqual(kwargs["text_format"], llm_query.QueryResponse)

    def test_main_reads_input_when_query_argument_missing(self) -> None:
        structured = llm_query.QueryResponse(
            type="person",
            details=llm_query.QueryDetails(
                role="director",
                name="Christopher Nolan",
                title=None,
                details=None,
            ),
        )

        with patch("builtins.input", return_value="who is Christopher Nolan"), patch(
            "scripts.llm_query.send_query_to_openai",
            return_value=structured,
        ) as mocked_send, patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = llm_query.main([])

        self.assertEqual(exit_code, 0)
        self.assertIn('"type":"person"', stdout.getvalue())
        mocked_send.assert_called_once()

    def test_main_returns_error_for_blank_query(self) -> None:
        with patch("builtins.input", return_value="  "), patch(
            "scripts.llm_query.send_query_to_openai",
        ) as mocked_send:
            exit_code = llm_query.main([])

        self.assertEqual(exit_code, 1)
        mocked_send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
