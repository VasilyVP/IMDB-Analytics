"""CLI helper for classifying movie-related user queries with a local Ollama model.

Quick start:
1) Start Ollama and make sure the model exists (default: llama3.2:3b).
2) Run:
   uv run scripts/llm_query.py --query "movies like Inception"

Alternative usage:
- Interactive prompt mode:
  uv run scripts/llm_query.py
- Custom model/host/timeout:
  uv run scripts/llm_query.py -q "who is Christopher Nolan" --model llama3.2:3b --host http://localhost:11434 --timeout 20
"""

from __future__ import annotations

import argparse
import sys
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel

SYSTEM_PROMPT = """You are an intent classification and information extraction system for a film database.

Your task:
Analyze a user query and return a structured JSON response.

You MUST:
- Classify the query into ONE of the following types:
  - "person_search"      (searching for a person by description)
  - "film_search"        (searching for a film by description)
  - "person"             (asking about a specific person)
  - "film"               (asking about a specific film)
  - "recommendation"     (asking for suggestions or similar content)

Decision rules:
- If the query contains a specific name of a person -> type = "person"
- If the query contains a specific film title -> type = "film"
- If the query describes a person without naming them -> type = "person_search"
- If the query describes a film without naming it -> type = "film_search"
- If the query asks for similar films/persons or suggestions -> type = "recommendation"

Extraction rules:
- role: "actor" | "director" | null
  - Only set if explicitly stated or strongly implied (e.g., "movies by Tarantino" -> director)
- name: extract ONLY if a specific real person is mentioned, otherwise null
- title: extract ONLY if a specific film is mentioned, otherwise null
- details:
  - If type is "person_search", "film_search", or "recommendation":
    - Rewrite the query into a clean, short semantic search phrase
    - Remove filler words
    - Keep key attributes (genre, era, plot, traits, style)
  - If type is "person" or "film": set to null

Normalization rules:
- Output MUST be valid JSON
- Do NOT include explanations
- Do NOT include extra fields
- Use null (not empty string) when value is missing
- Keep "details" under 20 words

Output format:
{
  "type": "...",
  "details": {
    "role": "actor" | "director" | null,
    "name": string | null,
    "title": string | null,
    "details": string | null
  }
}

Examples:

Input: "movies like Inception"
Output:
{
  "type": "recommendation",
  "details": {
    "role": null,
    "name": null,
    "title": "Inception",
    "details": "movies similar to Inception, mind-bending sci-fi"
  }
}

Input: "who is Christopher Nolan"
Output:
{
  "type": "person",
  "details": {
    "role": "director",
    "name": "Christopher Nolan",
    "title": null,
    "details": null
  }
}

Input: "film about a man aging backwards"
Output:
{
  "type": "film_search",
  "details": {
    "role": null,
    "name": null,
    "title": null,
    "details": "film about man aging backwards"
  }
}

Input: "actor who played Iron Man"
Output:
{
  "type": "person_search",
  "details": {
    "role": "actor",
    "name": null,
    "title": null,
    "details": "actor who played Iron Man"
  }
}"""

DEFAULT_MODEL = "llama3.2:3b"
DEFAULT_HOST = "http://localhost:11434"
DEFAULT_TIMEOUT_SECONDS = 20.0


class QueryDetails(BaseModel):
  role: Literal["actor", "director"] | None
  name: str | None
  title: str | None
  details: str | None


class QueryResponse(BaseModel):
  type: Literal[
    "person_search",
    "film_search",
    "person",
    "film",
    "recommendation",
  ]
  details: QueryDetails


def build_prompt(user_query: str) -> str:
    return f'{SYSTEM_PROMPT}\n\nInput: "{user_query}"\nOutput:'


def send_query_to_openai(
    prompt: str,
    model: str,
    host: str,
    timeout_seconds: float,
) -> QueryResponse:
    base_url = f"{host.rstrip('/')}/v1"
    client = OpenAI(
        base_url=base_url,
        api_key="ollama",
        timeout=timeout_seconds,
    )

    try:
        response = client.responses.parse(
            model=model,
            input=prompt,
            text_format=QueryResponse,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Could not get a response from model at {base_url}: {exc}") from exc

    parsed = response.output_parsed
    if not isinstance(parsed, QueryResponse):
        raise RuntimeError("Model did not return the expected structured response.")

    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a user query to a local Ollama Llama model using a fixed system prompt.",
    )
    parser.add_argument(
        "-q",
        "--query",
        help="User query to classify. If omitted, you will be prompted.",
    )
    parser.add_argument(
        "-m",
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model name (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"OpenAI-compatible host URL (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    query = args.query if args.query is not None else input("Enter your query: ")
    query = query.strip()

    if not query:
        print("Query cannot be empty.", file=sys.stderr)
        return 1

    prompt = build_prompt(query)

    try:
      print("Sending query to model...", file=sys.stderr)

      response = send_query_to_openai(
            prompt=prompt,
            model=args.model,
            host=args.host,
            timeout_seconds=args.timeout,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(response.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())