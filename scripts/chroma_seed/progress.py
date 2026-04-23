from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tqdm import tqdm


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    processed: int
    total: int
    success: int
    failed: int
    elapsed_seconds: float
    generation_seconds: float
    chromadb_save_seconds: float
    duckdb_query_seconds: float


def create_overall_progress(total: int) -> tqdm[Any]:
    return tqdm(total=total, desc="Total", unit="record")


def create_batch_progress() -> tqdm[Any]:
    return tqdm(total=0, desc="Batch", unit="record", leave=False)


def render_runtime_stats(snapshot: ProgressSnapshot) -> str:
    avg = snapshot.elapsed_seconds / snapshot.processed if snapshot.processed else 0.0
    if snapshot.processed and snapshot.total > snapshot.processed:
        eta = avg * (snapshot.total - snapshot.processed)
    else:
        eta = 0.0
    return (
        f"\nprocessed={snapshot.processed}/{snapshot.total}"
        f"\nsuccess={snapshot.success}"
        f"\nfailed={snapshot.failed}"
        f"\navg_sec_per_record={avg:.2f}"
        f"\nelapsed_sec={snapshot.elapsed_seconds:.2f}"
        f"\neta_sec={eta:.2f}"
        f"\ngeneration_sec={snapshot.generation_seconds:.2f}"
        f"\nchromadb_save_sec={snapshot.chromadb_save_seconds:.2f}"
        f"\nduckdb_query_sec={snapshot.duckdb_query_seconds:.2f}"
    )
