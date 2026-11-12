"""Tests for fuzz/harvest_corpus.py (PYTHON-6178)."""

from __future__ import annotations

import sys
from hashlib import sha256
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "fuzz"))

import harvest_corpus


def write_unit(directory: Path, data: bytes) -> Path:
    """Write a unit named by its content digest, like libFuzzer does."""
    path = directory / sha256(data).hexdigest()
    path.write_bytes(data)
    return path


@pytest.fixture
def dirs(tmp_path: Path) -> dict[str, Path]:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    return {
        "corpus": corpus,
        "baseline": tmp_path / "baseline.txt",
        "existing": tmp_path / "committed",
        "out": tmp_path / "harvest",
        "summary": tmp_path / "summary.md",
    }


def harvest(dirs: dict[str, Path], max_units: int = 50, max_unit_bytes: int = 65536) -> int:
    return harvest_corpus.run(
        corpus_dir=dirs["corpus"],
        baseline=dirs["baseline"],
        existing=dirs["existing"],
        out=dirs["out"],
        summary=dirs["summary"],
        max_units=max_units,
        max_unit_bytes=max_unit_bytes,
    )


def test_harvests_new_units(dirs: dict[str, Path]) -> None:
    write_unit(dirs["corpus"], b"new-unit")

    count = harvest(dirs)

    assert count == 1
    harvested = list(dirs["out"].iterdir())
    assert [p.name for p in harvested] == [sha256(b"new-unit").hexdigest()]
    assert harvested[0].read_bytes() == b"new-unit"


def test_skips_baseline_units(dirs: dict[str, Path]) -> None:
    write_unit(dirs["corpus"], b"seeded")
    dirs["baseline"].write_text(sha256(b"seeded").hexdigest() + "\n")

    assert harvest(dirs) == 0
    assert list(dirs["out"].iterdir()) == []


def test_skips_committed_units(dirs: dict[str, Path]) -> None:
    dirs["existing"].mkdir()
    write_unit(dirs["existing"], b"already-committed")
    write_unit(dirs["corpus"], b"already-committed")

    assert harvest(dirs) == 0


def test_skips_oversize_units(dirs: dict[str, Path]) -> None:
    write_unit(dirs["corpus"], b"x" * 100)
    write_unit(dirs["corpus"], b"y" * 50)

    assert harvest(dirs, max_unit_bytes=99) == 1
    assert len(list(dirs["out"].iterdir())) == 1
    assert "per-unit cap" in dirs["summary"].read_text()


def test_per_run_cap_prefers_smallest(dirs: dict[str, Path]) -> None:
    write_unit(dirs["corpus"], b"a" * 30)
    write_unit(dirs["corpus"], b"b" * 10)
    write_unit(dirs["corpus"], b"c" * 20)

    assert harvest(dirs, max_units=2) == 2
    assert {p.read_bytes() for p in dirs["out"].iterdir()} == {b"b" * 10, b"c" * 20}
    assert "per-run cap (2)" in dirs["summary"].read_text()


def test_dedups_identical_candidates_within_run(dirs: dict[str, Path]) -> None:
    """Same contents under two names count once against the cap.

    libFuzzer digest-names corpus files, so the second name simulates a
    crash artifact.
    """
    dup = b"same" * 4
    distinct = b"distinct-unit-input"
    write_unit(dirs["corpus"], dup)
    (dirs["corpus"] / f"crash-{sha256(dup).hexdigest()[:8]}").write_bytes(dup)
    write_unit(dirs["corpus"], distinct)

    assert harvest(dirs, max_units=2) == 2
    assert {p.read_bytes() for p in dirs["out"].iterdir()} == {dup, distinct}
    assert "Units dropped over the per-run cap (2): 0" in dirs["summary"].read_text()


def test_no_new_units_writes_summary_and_empty_out(dirs: dict[str, Path]) -> None:
    assert harvest(dirs) == 0
    assert list(dirs["out"].iterdir()) == []
    assert dirs["summary"].is_file()


def test_rejects_nonpositive_max_units(monkeypatch, capsys, dirs: dict[str, Path]) -> None:
    """A cap of 0 or fewer is operator error; fail with an actionable message."""
    monkeypatch.setenv("FUZZ_CORPUS_MAX_UNITS", "0")
    _argv(monkeypatch, dirs)
    with pytest.raises(SystemExit):
        harvest_corpus.main()
    assert "--max-units must be at least 1" in capsys.readouterr().err


def test_rejects_nonpositive_max_unit_bytes(monkeypatch, capsys, dirs: dict[str, Path]) -> None:
    monkeypatch.setenv("FUZZ_CORPUS_MAX_UNIT_BYTES", "-1")
    _argv(monkeypatch, dirs)
    with pytest.raises(SystemExit):
        harvest_corpus.main()
    assert "--max-unit-bytes must be at least 1" in capsys.readouterr().err


def _argv(monkeypatch, dirs: dict[str, Path]) -> None:
    """Point main()'s argparse at the fixture directories."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "harvest_corpus.py",
            "--corpus-dir",
            str(dirs["corpus"]),
            "--baseline",
            str(dirs["baseline"]),
            "--existing",
            str(dirs["existing"]),
            "--out",
            str(dirs["out"]),
            "--summary",
            str(dirs["summary"]),
        ],
    )
