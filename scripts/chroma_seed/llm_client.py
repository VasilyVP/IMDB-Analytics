from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Callable, TypeVar, cast
from urllib import request as urllib_request

from .models import PersonRecord, TitleRecord
from .prompts import (
    build_person_description_prompt,
    build_person_embedding_prompt,
    build_title_description_prompt,
    build_title_embedding_prompt,
)


_RecordT = TypeVar("_RecordT", TitleRecord, PersonRecord)


def _empty_failure_messages() -> dict[str, str]:
    return {}




@dataclass(slots=True)
class GenerationResult:
    descriptions: dict[str, str]
    failed_ids: list[str]
    failure_messages: dict[str, str] = field(default_factory=_empty_failure_messages)


@dataclass(slots=True)
class TextGenerationClient:
    model: str
    base_url: str
    api_key: str | None
    max_retries: int
    human_max_tokens: int
    embedding_max_tokens: int
    inference_concurrency: int
    _client: object = field(init=False, repr=False)
    _use_chat_completions_endpoint: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._use_chat_completions_endpoint = self.base_url.rstrip("/").endswith(
            "/chat/completions"
        )
        if self._use_chat_completions_endpoint:
            self._client = None
            return

        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "The openai package is required for Chroma seed generation."
            ) from exc
        self._client = OpenAI(base_url=self.base_url, api_key=self.api_key)

    def generate_human_descriptions(self, titles: list[TitleRecord]) -> GenerationResult:
        # Backward-compatible alias for titles mode.
        return self.generate_title_human_descriptions(titles)

    def generate_embedding_descriptions(self, titles: list[TitleRecord]) -> GenerationResult:
        # Backward-compatible alias for titles mode.
        return self.generate_title_embedding_descriptions(titles)

    def generate_title_human_descriptions(self, titles: list[TitleRecord]) -> GenerationResult:
        return self._generate_descriptions(
            records=titles,
            build_prompt=build_title_description_prompt,
            get_record_id=lambda record: record.title_id,
            max_tokens=self.human_max_tokens,
        )

    def generate_title_embedding_descriptions(self, titles: list[TitleRecord]) -> GenerationResult:
        return self._generate_descriptions(
            records=titles,
            build_prompt=build_title_embedding_prompt,
            get_record_id=lambda record: record.title_id,
            max_tokens=self.embedding_max_tokens,
        )

    def generate_person_human_descriptions(self, persons: list[PersonRecord]) -> GenerationResult:
        return self._generate_descriptions(
            records=persons,
            build_prompt=build_person_description_prompt,
            get_record_id=lambda record: record.person_id,
            max_tokens=self.human_max_tokens,
        )

    def generate_person_embedding_descriptions(
        self,
        persons: list[PersonRecord],
    ) -> GenerationResult:
        return self._generate_descriptions(
            records=persons,
            build_prompt=build_person_embedding_prompt,
            get_record_id=lambda record: record.person_id,
            max_tokens=self.embedding_max_tokens,
        )

    def _generate_descriptions(
        self,
        records: list[_RecordT],
        build_prompt: Callable[[_RecordT], tuple[str, str]],
        get_record_id: Callable[[_RecordT], str],
        max_tokens: int,
    ) -> GenerationResult:
        if not records:
            return GenerationResult(descriptions={}, failed_ids=[])

        async def _run() -> GenerationResult:
            semaphore = asyncio.Semaphore(max(1, self.inference_concurrency))
            tasks = [
                asyncio.create_task(
                    self._generate_for_record(
                        semaphore=semaphore,
                        record=record,
                        build_prompt=build_prompt,
                        get_record_id=get_record_id,
                        max_tokens=max_tokens,
                    )
                )
                for record in records
            ]
            results = await asyncio.gather(*tasks)

            descriptions: dict[str, str] = {}
            failed_ids: list[str] = []
            failure_messages: dict[str, str] = {}
            for title_id, text, error in results:
                if text is not None:
                    descriptions[title_id] = text
                else:
                    failed_ids.append(title_id)
                    failure_messages[title_id] = error

            return GenerationResult(
                descriptions=descriptions,
                failed_ids=failed_ids,
                failure_messages=failure_messages,
            )

        return asyncio.run(_run())

    async def _generate_for_record(
        self,
        semaphore: asyncio.Semaphore,
        record: _RecordT,
        build_prompt: Callable[[_RecordT], tuple[str, str]],
        get_record_id: Callable[[_RecordT], str],
        max_tokens: int,
    ) -> tuple[str, str | None, str]:
        record_id = get_record_id(record)
        system_prompt, user_prompt = build_prompt(record)
        last_error = "unknown error"
        for _ in range(self.max_retries):
            try:
                async with semaphore:
                    result_text = await asyncio.to_thread(
                        self._request_completion,
                        system_prompt,
                        user_prompt,
                        max_tokens,
                    )
                if result_text.strip() == "":
                    raise ValueError("Empty completion response.")
                return record_id, result_text.strip(), ""
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)

        return record_id, None, last_error

    def _request_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
    ) -> str:
        if self._use_chat_completions_endpoint:
            return self._request_chat_completions_endpoint(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
            )

        response = self._client.chat.completions.create(  # type: ignore[union-attr]
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt.strip()},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0,
        )
        return _extract_response_text(cast(object, response))

    def _request_chat_completions_endpoint(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
    ) -> str:
        messages = [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": user_prompt},
        ]
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        request = urllib_request.Request(
            self.base_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib_request.urlopen(request, timeout=30.0) as response:
            raw_body = response.read().decode("utf-8")

        parsed = json.loads(raw_body)
        return _extract_response_text(parsed)


def _extract_response_text(response: object) -> str:
    if isinstance(response, dict):
        response_dict = cast(dict[str, object], response)
        choices_obj = response_dict.get("choices")
        if not isinstance(choices_obj, list) or not choices_obj:
            raise ValueError("Completion response did not include choices.")
        choices = cast(list[object], choices_obj)
        first_choice_obj = choices[0]
        if not isinstance(first_choice_obj, dict):
            raise ValueError("Completion response choice did not include message.")
        first_choice_dict = cast(dict[str, object], first_choice_obj)
        message_obj = first_choice_dict.get("message")
        if not isinstance(message_obj, dict):
            raise ValueError("Completion response choice did not include message.")
        message_dict = cast(dict[str, object], message_obj)
        content_obj = message_dict.get("content")
        if not isinstance(content_obj, str):
            raise ValueError("Completion response content must be plain text.")
        return content_obj

    choices_obj = getattr(response, "choices", None)
    if not isinstance(choices_obj, list) or not choices_obj:
        raise ValueError("Completion response did not include choices.")
    choices = cast(list[object], choices_obj)
    first_choice = choices[0]
    message = getattr(first_choice, "message", None)
    if message is None:
        raise ValueError("Completion response choice did not include message.")

    content_obj = getattr(message, "content", None)
    if not isinstance(content_obj, str):
        raise ValueError("Completion response content must be plain text.")
    return content_obj
