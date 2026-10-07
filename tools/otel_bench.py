#!/usr/bin/env python
"""Benchmark the overhead of the driver's OpenTelemetry instrumentation.

Runs the DriverBench "Small doc insertOne" and "Find one by ID" tasks from
``test/performance`` (both the synchronous and the asynchronous API) under
each tracing configuration, interleaved with a rotated configuration order
across repetitions on a single host, and reports the overhead of each
configuration relative to the untraced baseline.

Implements the benchmarking requirements of the OpenTelemetry
specification's Performance Implications section (DRIVERS-3620):
https://github.com/mongodb/specifications/pull/1986

Configurations (per the spec's table; the harness, not the driver, installs
and configures the SDK and its sampler, and the SDK configurations install
no exporter or span processor. No configuration has an active parent span):

- ``off``        tracing disabled; the baseline.
- ``api-only``   tracing enabled; no SDK, so every tracing call is a no-op.
- ``sdk-ratio``  tracing enabled; SDK installed; ``TraceIdRatioBased(1%)`` sampler.
- ``sdk-always`` tracing enabled; SDK installed; every span sampled.
- ``main``       the commit before OpenTelemetry support was added, tracing
                 disabled (the spec's one-time pre-OTel comparison). Requires
                 ``--worktree-main`` pointing at a worktree of that commit.

Requires a MongoDB server on localhost:27017 (or via DB_IP/DB_PORT), the
DriverBench datasets (``--data-dir``), and ``opentelemetry-sdk`` installed
in the current environment (the SDK is benchmark tooling only, the driver
itself depends only on ``opentelemetry-api``).

Examples::

    python tools/otel_bench.py --verify
    python tools/otel_bench.py --fast --reps 1
    python tools/otel_bench.py --worktree-main /path/to/main-worktree
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent

# The two micro-benchmarks the spec requires, defined in both perf_test.py
# and async_perf_test.py.
TASKS = ("SmallDocInsertOne", "FindOneByID")

OFF = "off"
API_ONLY = "api-only"
SDK_RATIO = "sdk-ratio"
SDK_ALWAYS = "sdk-always"
MAIN = "main"

SDK_CONFIGS = frozenset({SDK_RATIO, SDK_ALWAYS})
ENABLED_CONFIGS = frozenset({API_ONLY, SDK_RATIO, SDK_ALWAYS})

BASE_CONFIGS = (OFF, API_ONLY, SDK_RATIO, SDK_ALWAYS, MAIN)

# Child pytest invocation; `-m perf` overrides the repo config's
# `-m default or default_async` addopts (the last -m wins).
PYTEST_ARGS = ["-m", "perf", "-v", "--durations=5"]

# opentelemetry.sdk is imported only for the SDK configurations. The provider
# is installed before pytest runs; the driver's cached ProxyTracer delegates
# to it regardless of import order. No span processor is added.
BOOTSTRAP = """import sys
config = sys.argv[1]
del sys.argv[1]
if config in ("sdk-ratio", "sdk-always"):
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON, TraceIdRatioBased

    sampler = TraceIdRatioBased(0.01) if config == "sdk-ratio" else ALWAYS_ON
    trace.set_tracer_provider(TracerProvider(sampler=sampler))
import pytest

sys.exit(pytest.main(sys.argv[1:]))
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reps", type=int, default=5, help="Number of interleaved repetitions")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Set FASTBENCH=1 in the benchmark children (quick, unrepresentative)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=ROOT / "specifications" / "source" / "benchmarking" / "data",
        help="Directory containing the DriverBench datasets",
    )
    parser.add_argument(
        "--worktree-main",
        type=Path,
        help="Worktree of the commit before OpenTelemetry support was added, "
        "for the spec's one-time pre-OTel comparison (the 'main' configuration)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "otel-bench-results",
        help="Directory for results and logs",
    )
    parser.add_argument("--configs", help="Comma-separated subset of: " + ",".join(BASE_CONFIGS))
    parser.add_argument(
        "--python", default=sys.executable, help="Python interpreter for the benchmark children"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only check that tracing produces spans end-to-end (with an in-memory exporter) and exit",
    )
    return parser.parse_args()


def resolve_configs(args: argparse.Namespace) -> tuple[str, ...]:
    configs = list(BASE_CONFIGS) if args.worktree_main else [c for c in BASE_CONFIGS if c != MAIN]
    if args.configs:
        selected = args.configs.split(",")
        unknown = set(selected) - set(configs)
        if unknown:
            raise SystemExit(f"Unknown or unavailable configurations: {sorted(unknown)}")
        configs = selected
    if OFF not in configs:
        raise SystemExit("The 'off' configuration is required as the baseline")
    return tuple(configs)


def build_command(config: str, api: str, python: str) -> list[str]:
    perf_file = (
        "test/performance/perf_test.py" if api == "sync" else "test/performance/async_perf_test.py"
    )
    nodes = [f"{perf_file}::Test{task}" for task in TASKS]
    if config in SDK_CONFIGS:
        return [python, "-c", BOOTSTRAP, config, *PYTEST_ARGS, *nodes]
    return [python, "-m", "pytest", *PYTEST_ARGS, *nodes]


def child_env(config: str, output_file: Path, args: argparse.Namespace) -> dict[str, str]:
    # The harness owns the tracing configuration: strip any ambient OTEL_
    # variables, then set exactly what the configuration requires.
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    env["TEST_PATH"] = str(args.data_dir)
    env["OUTPUT_FILE"] = str(output_file)
    env["PERF_CPU_TIME"] = "1"
    if args.fast:
        env["FASTBENCH"] = "1"
    if config in ENABLED_CONFIGS:
        env["OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"] = "1"
    return env


def run_one(config: str, api: str, rep: int, args: argparse.Namespace) -> tuple[Path, float]:
    output_file = args.output_dir / f"{api}-{config}-rep{rep:02d}.json"
    if output_file.exists():
        output_file.unlink()
    cmd = build_command(config, api, args.python)
    cwd = args.worktree_main if config == MAIN else ROOT
    log_file = output_file.with_suffix(".log")
    start = time.monotonic()
    with open(log_file, "w") as log:
        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            env=child_env(config, output_file, args),
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        print(f"FAILED (exit {proc.returncode}): {' '.join(cmd)}\nLog tail:")
        print("\n".join(log_file.read_text().splitlines()[-40:]))
        raise SystemExit(1)
    return output_file, elapsed


def parse_results(output_file: Path) -> dict[str, dict[str, float]]:
    entries = json.loads(output_file.read_text())
    parsed: dict[str, dict[str, float]] = {}
    for entry in entries:
        name = entry["info"]["test_name"]
        parsed[name] = {metric["name"]: metric["value"] for metric in entry["metrics"]}
    return parsed


def verify() -> None:
    """Check end-to-end that tracing produces spans, using an in-memory exporter.

    This is a wiring check only; measured runs never install an exporter.
    """
    os.environ["OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED"] = "1"
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    try:
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    except ImportError:
        # The module name changed across SDK versions.
        from opentelemetry.sdk.trace.export.in_memory_exporter import (  # type: ignore[import-not-found,no-redef]
            InMemorySpanExporter,
        )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    sys.path.insert(0, str(ROOT))
    from pymongo import MongoClient

    client: MongoClient[dict[str, Any]] = MongoClient()
    client.perftest_otel_verify.coll.insert_one({"x": 1})
    client.drop_database("perftest_otel_verify")
    client.close()

    spans = exporter.get_finished_spans()
    names = [span.name for span in spans]
    assert "insert" in names, f"Expected an 'insert' span, got: {names}"
    span = next(s for s in spans if s.name == "insert")
    attrs = dict(span.attributes or {})
    assert attrs.get("db.system.name") == "mongodb", attrs
    assert attrs.get("db.command.name") == "insert", attrs
    print(f"OK: {len(names)} span(s) recorded: {names}")


def report(
    configs: tuple[str, ...],
    scores: dict[tuple[str, str, str], list[float]],
    cpu_scores: dict[tuple[str, str, str], list[float]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    num_docs = 1000 if args.fast else 10000
    summary: dict[str, Any] = {
        "reps": args.reps,
        "fast": args.fast,
        "configs": {},
        "pre_otel_compare": None,
    }

    for api in ("sync", "async"):
        print(f"\n## {api} (median MB/s across repetitions, overhead vs 'off')")
        print("| config | " + " | ".join(TASKS) + " |")
        print("|" + "---|" * (len(TASKS) + 1))
        off_medians = {
            task: statistics.median(scores[(OFF, api, task)])
            for task in TASKS
            if scores.get((OFF, api, task))
        }
        for config in configs:
            if config == MAIN:
                continue
            cells = []
            for task in TASKS:
                values = scores.get((config, api, task), [])
                if not values:
                    cells.append("n/a")
                    continue
                median = statistics.median(values)
                if config == OFF:
                    cells.append(f"{median:.2f} MB/s (baseline)")
                else:
                    overhead = (1 - median / off_medians[task]) * 100
                    cells.append(f"{median:.2f} MB/s ({overhead:+.1f}%)")
            print(f"| {config} | " + " | ".join(cells) + " |")

        if MAIN in configs and all(scores.get((MAIN, api, task)) for task in TASKS):
            row: dict[str, Any] = {}
            for task in TASKS:
                main_median = statistics.median(scores[(MAIN, api, task)])
                off_median = statistics.median(scores[(OFF, api, task)])
                row[task] = {
                    "main_median_mb_s": main_median,
                    "off_median_mb_s": off_median,
                    "overhead_pct": (1 - off_median / main_median) * 100,
                }
            summary["pre_otel_compare"] = {"api": api, **row}
            overheads = ", ".join(f"{task}: {row[task]['overhead_pct']:+.1f}%" for task in TASKS)
            print(f"\nPre-OTel comparison (off on this branch vs main, {api}): {overheads}")

    print("\n## CPU time per operation (us/op median, delta vs 'off' in parens)")
    print(
        "| config | "
        + " | ".join(f"{api}/{task}" for api in ("sync", "async") for task in TASKS)
        + " |"
    )
    print("|" + "---|" * (len(TASKS) * 2 + 1))
    for config in configs:
        if config == MAIN:
            continue
        cells = []
        for api in ("sync", "async"):
            for task in TASKS:
                values = cpu_scores.get((config, api, task), [])
                base = cpu_scores.get((OFF, api, task), [])
                if not values or not base:
                    cells.append("n/a")
                    continue
                per_op = statistics.median(values) / num_docs * 1e6
                per_op_off = statistics.median(base) / num_docs * 1e6
                delta = "" if config == OFF else f" ({per_op - per_op_off:+.0f})"
                cells.append(f"{per_op:.0f}{delta}")
        print(f"| {config} | " + " | ".join(cells) + " |")

    for config in configs:
        summary["configs"][config] = {}
        for api in ("sync", "async"):
            for task in TASKS:
                values = scores.get((config, api, task), [])
                if values:
                    entry: dict[str, Any] = {
                        "mb_per_sec_reps": values,
                        "mb_per_sec_median": statistics.median(values),
                    }
                    cpu = cpu_scores.get((config, api, task), [])
                    if cpu:
                        entry["cpu_seconds_per_iteration_reps"] = cpu
                        entry["cpu_us_per_op_median"] = statistics.median(cpu) / num_docs * 1e6
                    summary["configs"][config][f"{api}/{task}"] = entry
    return summary


def main() -> None:
    args = parse_args()
    if args.verify:
        verify()
        return

    if args.reps < 1:
        raise SystemExit("--reps must be at least 1")
    if not args.data_dir.exists():
        raise SystemExit(
            f"Dataset directory not found: {args.data_dir}\n"
            "Clone the specifications repo and extract the data tarballs, or pass --data-dir."
        )
    configs = resolve_configs(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    scores: dict[tuple[str, str, str], list[float]] = {}
    cpu_scores: dict[tuple[str, str, str], list[float]] = {}
    started = time.monotonic()
    total_runs = args.reps * len(configs) * 2
    done_runs = 0
    summary: Optional[dict[str, Any]] = None

    for rep in range(1, args.reps + 1):
        # Rotate the configuration order each repetition so host drift affects
        # all configurations equally (the spec's interleaving requirement).
        order: deque[str] = deque(configs)
        order.rotate(-(rep - 1))
        for config in order:
            for api in ("sync", "async"):
                output_file, elapsed = run_one(config, api, rep, args)
                parsed = parse_results(output_file)
                for task in TASKS:
                    metrics = parsed.get(task, {})
                    mbps = metrics.get("megabytes_per_sec")
                    if mbps is not None:
                        scores.setdefault((config, api, task), []).append(mbps)
                    if "cpu_time_median" in metrics:
                        cpu_scores.setdefault((config, api, task), []).append(
                            metrics["cpu_time_median"]
                        )
                done_runs += 1
                results_mb = ", ".join(
                    f"{task}={parsed[task]['megabytes_per_sec']:.2f}MB/s" for task in TASKS
                )
                print(
                    f"[{done_runs}/{total_runs}] rep {rep}/{args.reps} {config:>9} {api:<5} "
                    f"({elapsed:.0f}s): {results_mb}",
                    flush=True,
                )
        # Crash-safe: persist intermediate results after every repetition.
        summary = report(configs, scores, cpu_scores, args)
        (args.output_dir / "otel_bench_results.json").write_text(json.dumps(summary, indent=2))

    assert summary is not None
    print(f"\nTotal: {time.monotonic() - started:.0f}s. Results in {args.output_dir}/")
    if summary["pre_otel_compare"]:
        print(json.dumps(summary["pre_otel_compare"], indent=2))


if __name__ == "__main__":
    main()
