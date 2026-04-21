from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import ChromaSeedRecord


@dataclass(slots=True)
class ChromaWriter:
    collection_name: str
    max_retries: int
    host: str
    port: int
    _client: Any = field(init=False, repr=False)
    _collection: Any | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        try:
            import chromadb  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "The chromadb package is required for Chroma seed writes."
            ) from exc

        self._client = chromadb.HttpClient(host=self.host, port=self.port)
        self._collection = None

    def ensure_collection(self, reset: bool) -> None:
        if reset:
            try:
                self._client.delete_collection(name=self.collection_name)
            except Exception:  # noqa: BLE001
                pass
        self._collection = self._client.get_or_create_collection(name=self.collection_name)

    def upsert_batch(self, records: list[ChromaSeedRecord]) -> None:
        if not records:
            return
        if self._collection is None:
            raise RuntimeError("Collection has not been initialized.")

        ids = [record.title_id for record in records]
        documents = [record.embedding_description for record in records]
        metadatas = [
            {
                "titleId": record.title_id,
                "title": record.title,
                "startYear": record.start_year,
                "human_description": record.human_description,
            }
            for record in records
        ]

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self._collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.max_retries:
                    break

        raise RuntimeError("Failed to write batch to ChromaDB.") from last_error
