"""Merges two benchmark results.json files into a side-by-side comparison.

Usage::

    python tools/compare_bson_perf.py results_c.json results_rust.json

Each input is the OUTPUT_FILE produced by ``test/performance/perf_test.py``
(a list of ``{"info": {"test_name": ...}, "metrics": [...]}`` entries).
The comparison is printed as a table and written to stdout in markdown.
"""

from __future__ import annotations

import json
import sys
from typing import Any


def load(path: str) -> list[dict[str, Any]]:
    with open(path) as fid:
        return json.load(fid)


def metrics_by_name(results: list[dict[str, Any]]) -> dict[str, float]:
    """Maps test name → megabytes_per_sec (one entry per test today)."""
    metrics: dict[str, float] = {}
    for entry in results:
        name = entry["info"]["test_name"]
        for metric in entry["metrics"]:
            if metric["name"] == "megabytes_per_sec":
                metrics[name] = metric["value"]
    return metrics


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    baseline = metrics_by_name(load(sys.argv[1]))
    challenger = metrics_by_name(load(sys.argv[2]))

    print(f"| {'Benchmark':<24} | {'C MB/s':>10} | {'Rust MB/s':>10} | {'Rust/C':>7} |")
    print(f"| {'-' * 24} | {'-' * 10} | {'-' * 10} | {'-' * 7} |")
    for name in sorted(baseline):
        base = baseline[name]
        challenger_value = challenger.get(name)
        if challenger_value is None:
            print(f"| {name:<24} | {base:>10.1f} | {'n/a':>10} | {'n/a':>7} |")
            continue
        ratio = "n/a" if base == 0 else f"{challenger_value / base:.2f}x"
        print(f"| {name:<24} | {base:>10.1f} | {challenger_value:>10.1f} | {ratio:>7} |")
    for name in sorted(challenger):
        if name not in baseline:
            print(f"| {name:<24} | {'n/a':>10} | {challenger[name]:>10.1f} | {'n/a':>7} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
