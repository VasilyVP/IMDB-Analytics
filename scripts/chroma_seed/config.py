from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]

# Keep existing environment variables as the source of truth.
load_dotenv(dotenv_path=REPO_ROOT / ".env", override=False)

DUCKDB_PATH = REPO_ROOT / "back-end" / "data" / "imdb.duckdb"
SQLITE_PATH = REPO_ROOT / "data" / "chroma_seed.sqlite"
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8001"))

COLLECTION_NAME = "titles"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
TEXT_GENERATION_PROVIDER = "OpenAI-compatible API"

TEXT_GENERATION_MODEL = os.getenv("TEXT_GENERATION_MODEL", "llama3.2:1b")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1/chat/completions")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "any-api-key")
HUMAN_MAX_TOKENS = 200
EMBEDDING_MAX_TOKENS = 250
INFERENCE_CONCURRENCY = int(os.getenv("INFERENCE_CONCURRENCY", str(BATCH_SIZE)))

MAX_RETRIES = 3
MAX_CONSECUTIVE_TITLE_FAILURES = 10


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    duckdb_path: Path
    sqlite_path: Path
    chroma_host: str
    chroma_port: int
    collection_name: str
    batch_size: int
    limit: int | None
    text_generation_provider: str
    model: str
    openai_base_url: str
    openai_api_key: str | None
    human_max_tokens: int
    embedding_max_tokens: int
    inference_concurrency: int
    max_retries: int
    max_consecutive_title_failures: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed ChromaDB with IMDB movie descriptions from DuckDB."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Records per batch.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for development/testing.",
    )
    return parser


def load_runtime_config(batch_size: int, limit: int | None) -> RuntimeConfig:
    inferred_concurrency = max(1, min(INFERENCE_CONCURRENCY, batch_size))
    return RuntimeConfig(
        duckdb_path=DUCKDB_PATH,
        sqlite_path=SQLITE_PATH,
        chroma_host=CHROMA_HOST,
        chroma_port=CHROMA_PORT,
        collection_name=COLLECTION_NAME,
        batch_size=batch_size,
        limit=limit,
        text_generation_provider=TEXT_GENERATION_PROVIDER,
        model=TEXT_GENERATION_MODEL,
        openai_base_url=OPENAI_BASE_URL,
        openai_api_key=OPENAI_API_KEY,
        human_max_tokens=HUMAN_MAX_TOKENS,
        embedding_max_tokens=EMBEDDING_MAX_TOKENS,
        inference_concurrency=inferred_concurrency,
        max_retries=MAX_RETRIES,
        max_consecutive_title_failures=MAX_CONSECUTIVE_TITLE_FAILURES,
    )
