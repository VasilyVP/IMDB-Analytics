from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "snapshots"
DEFAULT_CHROMA_CONTAINER_NAME = os.getenv("CHROMA_CONTAINER_NAME", "movies-chromadb")
DEFAULT_NEO4J_CONTAINER_NAME = os.getenv("NEO4J_CONTAINER_NAME", "movies-neo4j")
DEFAULT_CHUNK_SIZE = 1024 * 1024
BYTES_IN_MB = 1024 * 1024


@dataclass(frozen=True, slots=True)
class SnapshotTarget:
    container_name: str
    source_path: str
    archive_name: str


PROJECT_SNAPSHOT_TARGETS: tuple[SnapshotTarget, ...] = (
    SnapshotTarget(
        container_name=DEFAULT_NEO4J_CONTAINER_NAME,
        source_path="/data",
        archive_name="neo4j_data",
    ),
    SnapshotTarget(
        container_name=DEFAULT_NEO4J_CONTAINER_NAME,
        source_path="/logs",
        archive_name="neo4j_logs",
    ),
    SnapshotTarget(
        container_name=DEFAULT_CHROMA_CONTAINER_NAME,
        source_path="/data",
        archive_name="chromadb_data",
    ),
)


def _normalize_output_path(path: Path) -> Path:
    if path.suffix == ".tar":
        return path
    return path.with_name(f"{path.name}.tar")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create local tar snapshots for the project's container data directories.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where snapshot archives are written. Existing files are overwritten.",
    )
    return parser


def _write_snapshot_archive(
    container_name: str,
    source_path: str,
    output_path: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_callback: Callable[[int], None] | None = None,
) -> Path:
    return _write_snapshot_archive_chunked(
        container_name=container_name,
        source_path=source_path,
        output_path=output_path,
        chunk_size=chunk_size,
        progress_callback=progress_callback,
    )


def write_snapshot_archive(
    container_name: str,
    source_path: str,
    output_path: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_callback: Callable[[int], None] | None = None,
) -> Path:
    return _write_snapshot_archive(
        container_name=container_name,
        source_path=source_path,
        output_path=output_path,
        chunk_size=chunk_size,
        progress_callback=progress_callback,
    )


def _get_container_source_size(container_name: str, source_path: str) -> int | None:
    size_command = [
        "docker",
        "exec",
        container_name,
        "du",
        "-sb",
        source_path,
    ]
    completed = subprocess.run(
        args=size_command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    size_text = completed.stdout.decode("utf-8", errors="replace").strip()
    if not size_text:
        return None
    first_field = size_text.split()[0]
    return int(first_field)


def render_progress_line(
    archive_name: str,
    written_bytes: int,
    total_bytes: int | None,
) -> str:
    written_mb = written_bytes / BYTES_IN_MB
    if total_bytes is None or total_bytes <= 0:
        return f"\r{archive_name}: written {written_mb:.2f} MB"

    percent = min(100.0, (written_bytes / total_bytes) * 100.0)
    total_mb = total_bytes / BYTES_IN_MB
    return f"\r{archive_name}: {percent:6.2f}% ({written_mb:.2f}/{total_mb:.2f} MB)"


def _write_snapshot_archive_chunked(
    container_name: str,
    source_path: str,
    output_path: Path,
    chunk_size: int,
    progress_callback: Callable[[int], None] | None,
) -> Path:
    target_path = _normalize_output_path(output_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    total_bytes: int | None
    try:
        total_bytes = _get_container_source_size(container_name, source_path)
    except Exception:
        total_bytes = None

    command = [
        "docker",
        "exec",
        container_name,
        "tar",
        "-cf",
        "-",
        "-C",
        source_path,
        ".",
    ]

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if process.stdout is None:
        raise RuntimeError("Failed to open stdout pipe for snapshot process.")

    written_bytes = 0
    progress_label = target_path.name
    with open(target_path, "wb") as snapshot_file:
        while True:
            chunk = process.stdout.read(chunk_size)
            if not chunk:
                break
            snapshot_file.write(chunk)
            written_bytes += len(chunk)
            print(
                render_progress_line(
                    archive_name=progress_label,
                    written_bytes=written_bytes,
                    total_bytes=total_bytes,
                ),
                end="",
                flush=True,
            )
            if progress_callback is not None:
                progress_callback(written_bytes)

    return_code = process.wait()
    if return_code != 0:
        stderr_text = b""
        if process.stderr is not None:
            stderr_text = process.stderr.read()
        raise subprocess.CalledProcessError(
            returncode=return_code,
            cmd=command,
            output=None,
            stderr=stderr_text,
        )

    print(
        render_progress_line(
            archive_name=progress_label,
            written_bytes=written_bytes,
            total_bytes=total_bytes,
        ),
        flush=True,
    )

    return target_path


def backup_project_snapshots(output_dir: Path) -> list[Path]:
    snapshot_paths: list[Path] = []
    total_targets = len(PROJECT_SNAPSHOT_TARGETS)
    for index, target in enumerate(PROJECT_SNAPSHOT_TARGETS, start=1):
        print(
            f"[{index}/{total_targets}] Backing up {target.archive_name} "
            f"from {target.container_name}:{target.source_path}",
            flush=True,
        )
        snapshot_paths.append(
            _write_snapshot_archive(
                container_name=target.container_name,
                source_path=target.source_path,
                output_path=output_dir / target.archive_name,
            )
        )
    return snapshot_paths


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        snapshot_paths = backup_project_snapshots(output_dir=args.output_dir)
        print(f"Snapshots created in: {args.output_dir}")
        for snapshot_path in snapshot_paths:
            print(snapshot_path)
        return 0
    except FileNotFoundError:
        print("Docker CLI was not found in PATH.", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        stderr_text = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else ""
        print(
            f"Failed to create container snapshot. {stderr_text}".strip(),
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        print(f"Failed to write snapshot: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
