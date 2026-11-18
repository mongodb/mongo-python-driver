"""Tests for tools/synchro.py."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import synchro


def test_synchro(monkeypatch: pytest.MonkeyPatch) -> None:
    """Synchro processes only the async files passed as arguments.

    collection.py is a valid async source that would be processed when
    synchro runs with no arguments. When mongo_client.py is passed instead,
    collection.py must not reach unasync_directory.
    """
    monkeypatch.setattr("sys.argv", ["synchro.py", "pymongo/asynchronous/mongo_client.py"])
    monkeypatch.delenv("CI", raising=False)

    processed: list[str] = []

    def capture(files: list[str], *args: object, **kwargs: object) -> None:
        processed.extend(files)

    with (
        patch("synchro.unasync_directory", side_effect=capture),
        patch("synchro.process_files"),
        patch("synchro.prepare_staging"),
        patch("synchro.copy_changed_files", return_value=[]),
        patch("subprocess.run"),
        patch("pathlib.Path.write_text"),
    ):
        from synchro import main

        main()

    assert "./pymongo/asynchronous/mongo_client.py" in processed
    assert "./pymongo/asynchronous/collection.py" not in processed


def test_copy_changed_files_skips_identical(tmp_path: Path) -> None:
    """A destination whose content already matches is neither rewritten nor reported."""
    staged = tmp_path / "staged.py"
    dest = tmp_path / "dest.py"
    staged.write_bytes(b"same")
    dest.write_bytes(b"same")
    before = dest.stat().st_mtime_ns

    changed = synchro.copy_changed_files([(staged, dest)])

    assert changed == []
    assert dest.read_bytes() == b"same"
    assert dest.stat().st_mtime_ns == before


def test_copy_changed_files_updates_differing(tmp_path: Path) -> None:
    """A destination whose content differs is overwritten and reported."""
    staged = tmp_path / "staged.py"
    dest = tmp_path / "dest.py"
    staged.write_bytes(b"new")
    dest.write_bytes(b"old")

    changed = synchro.copy_changed_files([(staged, dest)])

    assert changed == [dest]
    assert dest.read_bytes() == b"new"
    # The backup is dropped once publication succeeds.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["dest.py", "staged.py"]


def test_copy_changed_files_creates_missing_dest(tmp_path: Path) -> None:
    """A missing destination is created (with parents) and reported."""
    staged = tmp_path / "staged.py"
    dest = tmp_path / "subdir" / "dest.py"
    staged.write_bytes(b"new")

    changed = synchro.copy_changed_files([(staged, dest)])

    assert changed == [dest]
    assert dest.read_bytes() == b"new"


def test_copy_changed_files_rolls_back_on_failure(tmp_path: Path) -> None:
    """A failed write restores destinations that were already written."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "blocking-dir" / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    # A file occupies b_dest.py's parent directory, so the mkdir fails.
    (tmp_path / "blocking-dir").write_bytes(b"not a directory")
    manifest = tmp_path / "manifest.txt"
    manifest.write_text("stale\n")

    with pytest.raises(FileExistsError):
        synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)], manifest=manifest)

    assert dest_a.read_bytes() == b"old-a"
    # A failed run claims nothing to stage.
    assert not manifest.exists()


def test_copy_changed_files_rollback_restores_in_reverse_order(tmp_path: Path) -> None:
    """Rollback frees later allocations first so earlier restores fit."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")
    staged_c = tmp_path / "c_staged.py"
    dest_c = tmp_path / "blocking-dir" / "c_dest.py"
    staged_c.write_bytes(b"new-c")
    # A file occupies c_dest.py's parent directory, so the mkdir fails.
    (tmp_path / "blocking-dir").write_bytes(b"not a directory")

    real_write = Path.write_bytes
    restored: list[bytes] = []

    def tracking_write(self: Path, data: bytes) -> None:
        if data == b"old-a" and b"old-b" not in restored:
            # Simulate ENOSPC: old-a does not fit until b's larger staged
            # content has been released by its own restore.
            raise OSError(28, "No space left on device")
        real_write(self, data)
        if data.startswith(b"old"):
            restored.append(data)

    # No hard-linkable backups: restores rewrite the files, so the
    # space-releasing order above is what the restores depend on.
    with patch("os.link", side_effect=OSError(1, "link not supported")):
        with patch.object(Path, "write_bytes", tracking_write):
            with pytest.raises(FileExistsError):
                synchro.copy_changed_files(
                    [(staged_a, dest_a), (staged_b, dest_b), (staged_c, dest_c)]
                )

    assert restored == [b"old-b", b"old-a"]
    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"old-b"


def test_copy_changed_files_failing_write_leaves_dest_unchanged(tmp_path: Path) -> None:
    """A write that fails midway leaves the failing destination unchanged."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")

    real_write = Path.write_bytes

    def failing_write(self: Path, data: bytes) -> None:
        if self.name.startswith(dest_b.name):
            # Simulate persistent ENOSPC: every write under dest_b's name,
            # including a rollback restore, lands partial bytes then fails.
            real_write(self, b"ne")
            raise OSError(28, "No space left on device")
        real_write(self, data)

    with patch.object(Path, "write_bytes", failing_write):
        with pytest.raises(OSError, match="No space left on device"):
            synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)])

    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"old-b"
    # No temporary or backup files are left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "a_dest.py",
        "a_staged.py",
        "b_dest.py",
        "b_staged.py",
    ]


def test_copy_changed_files_manifests_untracked_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unchanged untracked destination is listed in the manifest, not rewritten."""
    monkeypatch.chdir(tmp_path)
    staged = tmp_path / "staged.py"
    dest = tmp_path / "dest.py"
    staged.write_bytes(b"same")
    dest.write_bytes(b"same")
    manifest = tmp_path / "manifest.txt"
    before = dest.stat().st_mtime_ns

    changed = synchro.copy_changed_files(
        [(staged, Path("dest.py"))], manifest=manifest, tracked=set()
    )

    assert changed == []
    assert manifest.read_text() == "./dest.py\n"
    assert dest.read_bytes() == b"same"
    assert dest.stat().st_mtime_ns == before


def test_copy_changed_files_manifest_failure_rolls_back(tmp_path: Path) -> None:
    """A manifest write failure restores destinations and claims nothing to stage."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    manifest = tmp_path / "manifest.txt"
    manifest.write_text("stale\n")

    real_write_text = Path.write_text

    def failing_write_text(
        self: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        if self.name.startswith(manifest.name):
            # Simulate ENOSPC: partial bytes land before the write fails.
            real_write_text(self, data[:4], encoding, errors, newline)
            raise OSError(28, "No space left on device")
        return real_write_text(self, data, encoding, errors, newline)

    with patch.object(Path, "write_text", failing_write_text):
        with pytest.raises(OSError, match="No space left on device"):
            synchro.copy_changed_files([(staged_a, dest_a)], manifest=manifest)

    assert dest_a.read_bytes() == b"old-a"
    # A failed run claims nothing to stage.
    assert not manifest.exists()
    # No temporary files are left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "a_dest.py",
        "a_staged.py",
    ]


def test_copy_changed_files_interrupt_rolls_back(tmp_path: Path) -> None:
    """An interrupt mid-publication still restores destinations already replaced."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")

    real_write = Path.write_bytes

    def interrupting_write(self: Path, data: bytes) -> None:
        if self.name.startswith(dest_b.name):
            raise KeyboardInterrupt
        real_write(self, data)

    with patch.object(Path, "write_bytes", interrupting_write):
        with pytest.raises(KeyboardInterrupt):
            synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)])

    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"old-b"
    # No temporary files are left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "a_dest.py",
        "a_staged.py",
        "b_dest.py",
        "b_staged.py",
    ]


def test_copy_changed_files_interrupt_after_replace_rolls_back(tmp_path: Path) -> None:
    """An interrupt just after a replacement still rolls that destination back."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")

    real_replace = Path.replace

    def replacing_interrupt(self: Path, target: Path) -> Path:
        real_replace(self, target)
        if target == dest_b:
            raise KeyboardInterrupt
        return target

    with patch.object(Path, "replace", replacing_interrupt):
        with pytest.raises(KeyboardInterrupt):
            synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)])

    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"old-b"


def test_copy_changed_files_never_published_dest_not_restored(tmp_path: Path) -> None:
    """A destination whose replacement never lands is not rewritten by rollback."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")

    real_write = Path.write_bytes

    def failing_write(self: Path, data: bytes) -> None:
        if self.name.startswith(dest_b.name):
            # Simulate persistent ENOSPC: every write under dest_b's name,
            # including a rollback restore, lands partial bytes then fails.
            real_write(self, b"ne")
            raise OSError(28, "No space left on device")
        real_write(self, data)

    # No hard-link support: rollback restores would have to rewrite files.
    with patch("os.link", side_effect=OSError(1, "link not supported")):
        with patch.object(Path, "write_bytes", failing_write):
            with pytest.raises(OSError, match="No space left on device"):
                synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)])

    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"old-b"
    # No temporary or backup files are left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "a_dest.py",
        "a_staged.py",
        "b_dest.py",
        "b_staged.py",
    ]


def test_copy_changed_files_failed_restore_keeps_backup(tmp_path: Path) -> None:
    """A restore that fails keeps its backup as the recoverable copy."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")
    manifest = tmp_path / "manifest.txt"
    manifest.write_text("stale\n")

    real_replace = Path.replace
    real_write_text = Path.write_text

    def failing_restore(self: Path, target: Path) -> Path:
        if self.name.endswith(".synchro-backup") and target == dest_b:
            raise OSError(13, "Permission denied")
        return real_replace(self, target)

    def failing_write_text(
        self: Path,
        data: str,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> int:
        if self.name.startswith(manifest.name):
            raise OSError(28, "No space left on device")
        return real_write_text(self, data, encoding, errors, newline)

    with (
        patch.object(Path, "replace", failing_restore),
        patch.object(Path, "write_text", failing_write_text),
    ):
        with pytest.raises(OSError, match="No space left on device"):
            synchro.copy_changed_files([(staged_a, dest_a), (staged_b, dest_b)], manifest=manifest)

    assert dest_a.read_bytes() == b"old-a"
    assert dest_b.read_bytes() == b"new-b"
    # The failed restore's backup survives as the only copy of the original.
    backup_b = tmp_path / "b_dest.py.synchro-backup"
    assert backup_b.read_bytes() == b"old-b"
    assert not manifest.exists()


def test_copy_changed_files_unrecovered_backup_blocks_publication(tmp_path: Path) -> None:
    """An existing backup holding an unrecovered original refuses publication."""
    staged = tmp_path / "staged.py"
    dest = tmp_path / "dest.py"
    staged.write_bytes(b"new")
    dest.write_bytes(b"current")
    # A failed run left this backup holding the only copy of the original.
    (tmp_path / "dest.py.synchro-backup").write_bytes(b"original")

    with pytest.raises(OSError, match="unrecovered original"):
        synchro.copy_changed_files([(staged, dest)])

    assert dest.read_bytes() == b"current"
    assert (tmp_path / "dest.py.synchro-backup").read_bytes() == b"original"


def test_copy_changed_files_clears_stale_twin_backup(tmp_path: Path) -> None:
    """A stale backup that still matches the destination is safely replaced."""
    staged = tmp_path / "staged.py"
    dest = tmp_path / "dest.py"
    staged.write_bytes(b"new")
    dest.write_bytes(b"old")
    os.link(dest, tmp_path / "dest.py.synchro-backup")

    changed = synchro.copy_changed_files([(staged, dest)])

    assert changed == [dest]
    assert dest.read_bytes() == b"new"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["dest.py", "staged.py"]


def test_copy_changed_files_backup_cleanup_failure_is_not_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A backup that cannot be swept after publication does not fail the run."""
    monkeypatch.chdir(tmp_path)
    staged = tmp_path / "staged.py"
    dest = Path("dest.py")
    staged.write_bytes(b"new")
    dest.write_bytes(b"old")
    manifest = tmp_path / "manifest.txt"

    real_unlink = Path.unlink
    backup_unlinks = 0

    def failing_unlink(self: Path, missing_ok: bool = False) -> None:
        nonlocal backup_unlinks
        if self.name.endswith(".synchro-backup"):
            backup_unlinks += 1
            if backup_unlinks > 1:
                # The first unlink is the stale-backup clear at creation;
                # the second is the success sweep.
                raise OSError(13, "Permission denied")
        real_unlink(self, missing_ok=missing_ok)

    with patch.object(Path, "unlink", failing_unlink):
        changed = synchro.copy_changed_files([(staged, dest)], manifest=manifest)

    assert changed == [dest]
    assert dest.read_bytes() == b"new"
    assert manifest.read_text() == "./dest.py\n"
    # The backup remains, with a warning on stderr.
    assert (tmp_path / "dest.py.synchro-backup").exists()
    assert "could not remove" in capsys.readouterr().err


def test_copy_changed_files_failed_fallback_restore_is_clean(tmp_path: Path) -> None:
    """A fallback restore that fails leaves the published content in place."""
    staged_a = tmp_path / "a_staged.py"
    dest_a = tmp_path / "a_dest.py"
    staged_a.write_bytes(b"new-a")
    dest_a.write_bytes(b"old-a")
    staged_b = tmp_path / "b_staged.py"
    dest_b = tmp_path / "b_dest.py"
    staged_b.write_bytes(b"new-b")
    dest_b.write_bytes(b"old-b")
    staged_c = tmp_path / "c_staged.py"
    dest_c = tmp_path / "blocking-dir" / "c_dest.py"
    staged_c.write_bytes(b"new-c")
    # A file occupies c_dest.py's parent directory, so the mkdir fails.
    (tmp_path / "blocking-dir").write_bytes(b"not a directory")

    real_write = Path.write_bytes

    def failing_restore(self: Path, data: bytes) -> None:
        if data == b"old-b":
            # Simulate persistent ENOSPC: the restore lands partial bytes
            # then fails.
            real_write(self, b"ol")
            raise OSError(28, "No space left on device")
        real_write(self, data)

    # No hard-link support: restores must rewrite the files.
    with patch("os.link", side_effect=OSError(1, "link not supported")):
        with patch.object(Path, "write_bytes", failing_restore):
            with pytest.raises(FileExistsError):
                synchro.copy_changed_files(
                    [(staged_a, dest_a), (staged_b, dest_b), (staged_c, dest_c)]
                )

    assert dest_a.read_bytes() == b"old-a"
    # The failed restore leaves the published content, uncorrupted.
    assert dest_b.read_bytes() == b"new-b"
    # No temporary or backup files are left behind.
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "a_dest.py",
        "a_staged.py",
        "b_dest.py",
        "b_staged.py",
        "blocking-dir",
        "c_staged.py",
    ]


ASYNC_SOURCE = "_IS_SYNC = False\n\n\ndef f() -> None:\n    pass\n"
SYNC_SOURCE = "_IS_SYNC = True\n\n\ndef f() -> None:\n    pass\n"


def setup_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a minimal repo-like tree and point synchro at it."""
    (tmp_path / "pymongo" / "asynchronous").mkdir(parents=True)
    (tmp_path / "pymongo" / "synchronous").mkdir(parents=True)
    (tmp_path / "test" / "asynchronous").mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text("")
    (tmp_path / "pymongo" / "asynchronous" / "foo.py").write_text(ASYNC_SOURCE)
    monkeypatch.setattr(sys, "argv", ["synchro.py"])
    monkeypatch.setattr(synchro, "async_files", ["./pymongo/asynchronous/foo.py"])
    monkeypatch.setattr(synchro, "gridfs_files", [])
    monkeypatch.setattr(synchro, "test_files", [])
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path / "pymongo" / "synchronous" / "foo.py"


def test_noop_run_leaves_tree_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A run with nothing to regenerate writes no files and stages nothing."""
    dest = setup_tree(tmp_path, monkeypatch)
    dest.write_text(SYNC_SOURCE)
    before = dest.stat().st_mtime_ns

    with (
        patch("subprocess.run"),
        patch("synchro.git_tracked_files", return_value=None),
    ):
        synchro.main()

    assert dest.read_text() == SYNC_SOURCE
    assert dest.stat().st_mtime_ns == before
    assert (tmp_path / ".synchro-modified").read_text() == ""
    assert not (tmp_path / ".synchro-staging").exists()


def test_regenerated_file_is_copied_and_manifested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only files whose content changed are copied and listed in the manifest."""
    dest = setup_tree(tmp_path, monkeypatch)
    dest.write_text("_IS_SYNC = True\n# stale\n")

    with (
        patch("subprocess.run"),
        patch("synchro.git_tracked_files", return_value=None),
    ):
        synchro.main()

    assert dest.read_text() == SYNC_SOURCE
    assert (tmp_path / ".synchro-modified").read_text() == "./pymongo/synchronous/foo.py\n"
    assert not (tmp_path / ".synchro-staging").exists()


def test_async_test_without_counterpart_is_not_mirrored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An async test file is mirrored only when test/<name>.py already exists."""
    setup_tree(tmp_path, monkeypatch)
    (tmp_path / "test" / "asynchronous" / "test_bar.py").write_text(ASYNC_SOURCE)
    monkeypatch.setattr(synchro, "test_files", ["./test/asynchronous/test_bar.py"])

    with (
        patch("subprocess.run"),
        patch("synchro.git_tracked_files", return_value=None),
    ):
        synchro.main()

    assert not (tmp_path / "test" / "test_bar.py").exists()


def test_ruff_failure_leaves_tree_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ruff failure on generated output exits nonzero without touching the tree."""
    dest = setup_tree(tmp_path, monkeypatch)
    dest.write_text("_IS_SYNC = True\n# stale\n")
    (tmp_path / ".synchro-modified").write_text("./pymongo/synchronous/foo.py\n")

    def failing_ruff(
        cmd: list[str], *args: object, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if "--fix" in cmd:
            raise subprocess.CalledProcessError(1, cmd)
        return subprocess.CompletedProcess(cmd, 0)

    with patch("subprocess.run", side_effect=failing_ruff):
        with pytest.raises(SystemExit) as excinfo:
            synchro.main()

    assert excinfo.value.code == 1
    assert dest.read_text() == "_IS_SYNC = True\n# stale\n"
    assert not (tmp_path / ".synchro-modified").exists()
    assert not (tmp_path / ".synchro-staging").exists()


def test_early_failure_claims_nothing_to_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure before publication leaves no manifest to stage."""
    setup_tree(tmp_path, monkeypatch)
    (tmp_path / ".synchro-modified").write_text("./pymongo/synchronous/foo.py\n")

    def failing_first_ruff(
        cmd: list[str], *args: object, **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, cmd)

    with patch("subprocess.run", side_effect=failing_first_ruff):
        with pytest.raises(subprocess.CalledProcessError):
            synchro.main()

    assert not (tmp_path / ".synchro-modified").exists()


def test_ruff_commands_keep_diagnostics_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ruff runs without --silent so fatal failures show their diagnostics."""
    setup_tree(tmp_path, monkeypatch)

    commands: list[list[str]] = []

    def capture(cmd: list[str], *args: object, **kwargs: object) -> None:
        commands.append(cmd)

    with (
        patch("subprocess.run", side_effect=capture),
        patch("synchro.git_tracked_files", return_value=None),
    ):
        synchro.main()

    ruff_cmds = [cmd for cmd in commands if "ruff" in cmd]
    assert ruff_cmds
    for cmd in ruff_cmds:
        assert "--silent" not in cmd


def test_ci_reports_drift(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """In CI, changed generated files are copied so the drift is visible, then fail."""
    dest = setup_tree(tmp_path, monkeypatch)
    dest.write_text("_IS_SYNC = True\n# stale\n")
    monkeypatch.setenv("CI", "true")

    with patch("subprocess.run"):
        with pytest.raises(SystemExit) as excinfo:
            synchro.main()

    assert excinfo.value.code == 1
    assert dest.read_text() == SYNC_SOURCE


def test_mirrored_test_files_requires_sync_counterpart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async tests are mirrored only when the sync counterpart exists."""
    async_dir = tmp_path / "test" / "asynchronous"
    async_dir.mkdir(parents=True)
    (async_dir / "test_a.py").write_text("")
    (async_dir / "test_b.py").write_text("")
    (tmp_path / "test" / "test_a.py").write_text("")
    monkeypatch.setattr(synchro, "_test_dest_base", f"{tmp_path / 'test'}/")

    filtered = [str(async_dir / "test_a.py"), str(async_dir / "test_b.py")]

    assert synchro.mirrored_test_files(filtered) == [f"{tmp_path / 'test' / 'test_a.py'}"]


def test_git_tracked_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """git_tracked_files reports tracked paths and None outside a repository."""
    monkeypatch.chdir(tmp_path)
    assert synchro.git_tracked_files([Path("a.py")]) is None

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "a.py").write_bytes(b"a")
    (tmp_path / "b.py").write_bytes(b"b")
    # git ls-files reports the index, so staging a.py marks it tracked.
    subprocess.run(["git", "add", "a.py"], cwd=tmp_path, check=True)

    assert synchro.git_tracked_files([Path("a.py"), Path("b.py")]) == {Path("a.py")}


def test_prepare_staging_pins_first_party_packages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The staged pyproject copy pins the first-party packages for isort."""
    (tmp_path / "pyproject.toml").write_text(
        '[tool.ruff.lint]\nisort.required-imports = ["from __future__ import annotations"]\n'
    )
    monkeypatch.chdir(tmp_path)

    synchro.prepare_staging()

    staged = (tmp_path / synchro.STAGING / "pyproject.toml").read_text()
    assert 'isort.known-first-party = ["pymongo", "gridfs", "bson", "test"]' in staged
    assert "isort.required-imports" in staged


def test_prepare_staging_warns_when_anchor_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A pyproject without the isort anchor gets no insert and a stderr warning."""
    (tmp_path / "pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)

    synchro.prepare_staging()

    staged = (tmp_path / synchro.STAGING / "pyproject.toml").read_text()
    assert "known-first-party" not in staged
    assert "known-first-party" in capsys.readouterr().err
