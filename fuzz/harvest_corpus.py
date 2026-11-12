#!/usr/bin/env python3
"""
Harvest coverage-increasing inputs from a finished fuzzing run (PYTHON-6178).

Compares the scratch corpus directory (where libFuzzer writes each new
"interesting" input) against the baseline manifest written by
setup_tests.py and the committed fuzz/corpus/ contents, copies the new
units into an output directory named by their SHA-256 digests, and writes
a Markdown summary for the automated corpus PR body.

Pure stdlib: this runs on the Evergreen nightly host after the fuzz
targets finish, and its unit tests must run everywhere (no atheris, no
bson).

Usage: python harvest_corpus.py [--corpus-dir DIR] [--baseline FILE]
"""

from __future__ import annotations

import argparse
import os
import shutil
from hashlib import sha256
from pathlib import Path

DEFAULT_CORPUS_DIR = "fuzz-scratch/corpus"
DEFAULT_EXISTING = "fuzz/corpus"
DEFAULT_OUT = "fuzz-scratch/harvest"
DEFAULT_SUMMARY = "fuzz-scratch/harvest-summary.md"
# Growth caps keep a runaway run from flooding the repository; both are
# overridable from Evergreen. Defaults chosen in PYTHON-6178: small inputs
# carry the most coverage per committed byte.
DEFAULT_MAX_UNITS = 50
DEFAULT_MAX_UNIT_BYTES = 64 * 1024


def digest_of(path: Path) -> str:
    """The SHA-256 hex digest of a file's contents."""
    digest = sha256()
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_set(directory: Path) -> set[str]:
    """Content digests of every file in directory, skipping dotfiles."""
    if not directory.is_dir():
        return set()
    return {
        digest_of(path)
        for path in sorted(directory.iterdir())
        if path.is_file() and not path.name.startswith(".")
    }


def read_baseline(path: Path) -> set[str]:
    """Read digests from a baseline manifest (one hex digest per line)."""
    if not path.is_file():
        return set()
    with open(path, encoding="utf-8") as fp:
        return {line.strip() for line in fp if line.strip()}


def run(
    corpus_dir: Path,
    baseline: Path,
    existing: Path,
    out: Path,
    summary: Path,
    max_units: int,
    max_unit_bytes: int,
) -> int:
    """Copy the new units into out/ and write the PR summary.

    Returns the number of units harvested; zero means the run found
    nothing new and the PR step can no-op.
    """
    known = read_baseline(baseline) | digest_set(existing)

    # Sorted (size, digest, path): the size key prefers the smallest
    # inputs when the per-run cap bites.
    candidates: list[tuple[int, str, Path]] = []
    oversize = 0
    for path in sorted(corpus_dir.iterdir()):
        if path.name.startswith(".") or not path.is_file():
            continue
        digest = digest_of(path)
        if digest in known:
            continue
        # Track accepted digests: same contents under two names count once.
        known.add(digest)
        size = path.stat().st_size
        if size > max_unit_bytes:
            oversize += 1
            continue
        candidates.append((size, digest, path))
    candidates.sort()

    kept = candidates[:max_units]
    over_cap = len(candidates) - len(kept)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    for _, digest, path in kept:
        shutil.copyfile(path, out / digest)

    write_summary(summary, kept, oversize, over_cap, max_units, max_unit_bytes)
    print(
        f"Harvested {len(kept)} new units "
        f"({oversize} oversize skipped, {over_cap} over the per-run cap)."
    )
    return len(kept)


def write_summary(
    path: Path,
    kept: list[tuple[int, str, Path]],
    oversize: int,
    over_cap: int,
    max_units: int,
    max_unit_bytes: int,
) -> None:
    """Write the Markdown harvest summary used as the automated PR body."""
    lines = [
        "Harvested coverage-increasing inputs from last night's BSON fuzz run.",
        "",
        f"- New units added: **{len(kept)}**",
        f"- Units skipped over the per-unit cap ({max_unit_bytes} bytes): {oversize}",
        f"- Units dropped over the per-run cap ({max_units}): {over_cap}",
        "",
        (
            "These are fuzzer-generated inputs, not reviewed code. Before merging, "
            "skim them for size and for inputs that look like a bug rather than a "
            "plain coverage gain; anything crashing belongs in `fuzz/regression/`."
        ),
    ]
    if kept:
        lines.extend(["", "Largest units added:"])
        for size, digest, _ in sorted(kept, reverse=True)[:5]:
            lines.append(f"- `{digest[:16]}` ({size} bytes)")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Harvest new coverage-increasing inputs from a fuzz run."
    )
    parser.add_argument(
        "--corpus-dir",
        default=os.environ.get("FUZZ_CORPUS_DIR", DEFAULT_CORPUS_DIR),
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Digest manifest; defaults to baseline.txt next to the corpus dir.",
    )
    parser.add_argument("--existing", default=DEFAULT_EXISTING)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--summary", default=DEFAULT_SUMMARY)
    parser.add_argument(
        "--max-units",
        type=int,
        default=int(os.environ.get("FUZZ_CORPUS_MAX_UNITS", DEFAULT_MAX_UNITS)),
    )
    parser.add_argument(
        "--max-unit-bytes",
        type=int,
        default=int(os.environ.get("FUZZ_CORPUS_MAX_UNIT_BYTES", DEFAULT_MAX_UNIT_BYTES)),
    )
    args = parser.parse_args()
    # Negative or zero caps are operator error (e.g. a mis-set Evergreen
    # expansion): fail with an actionable message rather than a nonsense
    # keep-set like "all but the last N".
    if args.max_units < 1:
        parser.error("--max-units must be at least 1")
    if args.max_unit_bytes < 1:
        parser.error("--max-unit-bytes must be at least 1")
    corpus_dir = Path(args.corpus_dir)
    baseline = Path(args.baseline) if args.baseline else corpus_dir.parent / "baseline.txt"
    run(
        corpus_dir=corpus_dir,
        baseline=baseline,
        existing=Path(args.existing),
        out=Path(args.out),
        summary=Path(args.summary),
        max_units=args.max_units,
        max_unit_bytes=args.max_unit_bytes,
    )


if __name__ == "__main__":
    main()
