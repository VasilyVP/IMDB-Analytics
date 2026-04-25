from __future__ import annotations

import sys
from pathlib import Path

from app.core.config import get_settings


def _ensure_repo_root_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.append(repo_root_str)


_ensure_repo_root_on_path()

from scripts.chroma_seed.llm_client import TextGenerationClient  # noqa: E402


def generate_description(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
) -> str:
    """Generate a description by calling LLM with given prompts.
    
    Args:
        system_prompt: System prompt for the LLM
        user_prompt: User prompt for the LLM
        max_tokens: Maximum tokens for the response
        
    Returns:
        Generated description text
        
    Raises:
        RuntimeError: If generation fails
    """
    client = _build_client()
    try:
        result_text = client._request_completion(  # type: ignore[private-access]
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=max_tokens,
        )
        if result_text.strip() == "":
            raise RuntimeError("Empty completion response")
        return result_text.strip()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Description generation failed: {exc}") from exc


def _build_client() -> TextGenerationClient:
    settings = get_settings()
    return TextGenerationClient(
        model=settings.TEXT_GENERATION_MODEL,
        base_url=settings.OPENAI_BASE_URL,
        api_key=settings.OPENAI_API_KEY,
        max_retries=settings.LLM_MAX_RETRIES,
        human_max_tokens=settings.HUMAN_MAX_TOKENS,
        embedding_max_tokens=settings.EMBEDDING_MAX_TOKENS,
        inference_concurrency=1,
    )
