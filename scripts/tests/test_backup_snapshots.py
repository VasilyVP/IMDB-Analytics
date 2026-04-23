from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

_REPO_ROOT = Path(__file__).resolve().parents[2]
import sys

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.backup_snapshots as backup_snapshots  # noqa: E402


class BackupSnapshotsTests(unittest.TestCase):
    def test_render_progress_line_uses_mb_units(self) -> None:
        line = backup_snapshots.render_progress_line(
            archive_name="demo.tar",
            written_bytes=1024 * 1024,
            total_bytes=2 * 1024 * 1024,
        )

        self.assertIn("50.00%", line)
        self.assertIn("1.00/2.00 MB", line)

    def test_backup_project_snapshots_writes_all_archives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            observed_calls: list[tuple[str, str, str]] = []

            def _fake_write_snapshot_archive(
                container_name: str,
                source_path: str,
                output_path: Path,
            ) -> Path:
                observed_calls.append((container_name, source_path, output_path.name))
                output_path.with_suffix(".tar").write_bytes(
                    f"{container_name}:{source_path}".encode("utf-8")
                )
                return output_path.with_suffix(".tar")

            with patch(
                "scripts.backup_snapshots._write_snapshot_archive",
                side_effect=_fake_write_snapshot_archive,
            ):
                actual = backup_snapshots.backup_project_snapshots(
                    output_dir=output_dir,
                )

            self.assertEqual(
                observed_calls,
                [
                    ("movies-neo4j", "/data", "neo4j_data"),
                    ("movies-neo4j", "/logs", "neo4j_logs"),
                    ("movies-chromadb", "/data", "chromadb_data"),
                ],
            )
            self.assertEqual(
                [path.name for path in actual],
                ["neo4j_data.tar", "neo4j_logs.tar", "chromadb_data.tar"],
            )
            self.assertEqual(
                (output_dir / "neo4j_data.tar").read_bytes(),
                b"movies-neo4j:/data",
            )

    def test_backup_project_snapshots_overwrites_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            destination = output_dir / "chromadb_data.tar"
            destination.write_bytes(b"old-bytes")

            def _fake_write_snapshot_archive(
                container_name: str,
                source_path: str,
                output_path: Path,
            ) -> Path:
                target = output_path.with_suffix(".tar")
                if container_name == "movies-chromadb" and source_path == "/data":
                    target.write_bytes(b"new-bytes")
                else:
                    target.write_bytes(b"other-bytes")
                return target

            with patch(
                "scripts.backup_snapshots._write_snapshot_archive",
                side_effect=_fake_write_snapshot_archive,
            ):
                backup_snapshots.backup_project_snapshots(output_dir=output_dir)

            self.assertEqual(destination.read_bytes(), b"new-bytes")

    def test_write_snapshot_archive_copies_pipe_output_in_chunks_and_reports_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "chunked_dump"
            fake_stdout = Mock()
            fake_stdout.read = Mock(side_effect=[b"abc", b"defg", b""])
            fake_stderr = Mock()
            fake_stderr.read = Mock(return_value=b"")

            fake_process = Mock()
            fake_process.stdout = fake_stdout
            fake_process.stderr = fake_stderr
            fake_process.wait = Mock(return_value=0)

            progress_calls: list[int] = []

            fake_du_result = Mock()
            fake_du_result.stdout = b"7 /data\n"

            with patch("scripts.backup_snapshots.subprocess.Popen", return_value=fake_process) as popen_mock, patch(
                "scripts.backup_snapshots.subprocess.run",
                return_value=fake_du_result,
            ):
                result = backup_snapshots.write_snapshot_archive(
                    container_name="movies-chromadb",
                    source_path="/data",
                    output_path=output,
                    chunk_size=3,
                    progress_callback=lambda written_bytes: progress_calls.append(written_bytes),
                )

            self.assertEqual(result.name, "chunked_dump.tar")
            self.assertEqual(result.read_bytes(), b"abcdefg")
            self.assertEqual(progress_calls, [3, 7])
            popen_mock.assert_called_once_with(
                [
                    "docker",
                    "exec",
                    "movies-chromadb",
                    "tar",
                    "-cf",
                    "-",
                    "-C",
                    "/data",
                    ".",
                ],
                stdout=backup_snapshots.subprocess.PIPE,
                stderr=backup_snapshots.subprocess.PIPE,
            )

    def test_parser_defaults_to_repo_snapshot_directory(self) -> None:
        parser = backup_snapshots.build_parser()
        args = parser.parse_args([])

        self.assertEqual(args.output_dir, backup_snapshots.DEFAULT_OUTPUT_DIR)

    def test_main_returns_error_when_snapshot_raises(self) -> None:
        with patch(
            "scripts.backup_snapshots.backup_project_snapshots",
            side_effect=RuntimeError("boom"),
        ):
            actual = backup_snapshots.main([])

        self.assertEqual(actual, 1)


if __name__ == "__main__":
    unittest.main()
