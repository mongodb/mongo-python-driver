#!/bin/bash
# Compare BSON codec throughput: the C extension vs the Rust-backed codec.
#
# Provisions the perf test environment (venv, benchmark data), builds the
# Rust codec from rust/bson, runs the BSON micro-benchmarks once per codec,
# and prints a side-by-side comparison.
#
# Requires the MongoDB server started (the perf tests' setUpClass connects);
# the Evergreen task runs `run server` first. Locally:
#   MONGODB_VERSION=8.0 just run-server
set -euo pipefail

SCRIPT_DIR=$(dirname "${BASH_SOURCE:-$0}")
SCRIPT_DIR="$(cd "$SCRIPT_DIR" > /dev/null 2>&1 && pwd)"
ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
cd "$ROOT_DIR"

# Source (not execute) so the exports apply here: this puts the pinned uv
# (required-version in pyproject.toml) first on PATH and ensures it is
# installed — the host image's uv may be older and would abort every uv
# invocation. Sources env.sh/test-env.sh and runs install-dependencies.sh
# (idempotent; 'setup system' already ran it).
. "$SCRIPT_DIR/setup-dev-env.sh"

# Provision the perf environment: venv, benchmark data, test-env.sh.
bash "$SCRIPT_DIR/setup-tests.sh" perf sync

# Source the env written by setup_tests.py (TEST_PATH, UV_ARGS, ...).
if [ -f "$SCRIPT_DIR/test-env.sh" ]; then
  . "$SCRIPT_DIR/test-env.sh"
fi

# Ensure a Rust toolchain. Evergreen sets CARGO_HOME to
# $DRIVERS_TOOLS/.cargo (via env.sh from configure-env.sh); rustup defaults
# to $HOME/.cargo elsewhere — source the env from wherever it installed so
# cargo is on PATH.
if ! command -v cargo > /dev/null 2>&1; then
  echo "Installing Rust toolchain..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
_cargo_env="${CARGO_HOME:-$HOME/.cargo}/env"
if [ -f "$_cargo_env" ]; then
  # shellcheck disable=SC1090  # the env file's location depends on CARGO_HOME
  . "$_cargo_env"
fi
if ! command -v cargo > /dev/null 2>&1; then
  echo "cargo is not on PATH after the Rust toolchain setup" >&2
  exit 1
fi

# Sync the venv with the perf environment (pytest via the test extra,
# simplejson via the perf group). This must run BEFORE the codec build:
# uv sync prunes locally-installed packages that are not in uv.lock.
uv sync ${UV_ARGS:-}

# Build the Rust codec into the venv (after the sync — see above). Two uv
# pitfalls to avoid here: `uv run --with maturin` would run maturin inside
# a throwaway overlay environment (the codec lands in the overlay, not the
# venv), and uv run's implicit sync would prune the freshly built codec.
uv pip install maturin
# Target the building machine's CPU: this task builds with `maturin develop`
# on the same host that benchmarks (never cross-compiled or distributed), and
# the pip-built C extension only gets baseline codegen. Set via RUSTFLAGS —
# cargo only discovers a crate-local .cargo/config.toml from the invocation
# directory upward, which maturin (run from the checkout root) would miss.
export RUSTFLAGS="${RUSTFLAGS:-}-C target-cpu=native"
.venv/bin/maturin develop --release -m rust/bson/Cargo.toml

# Run the benchmarks with the venv's interpreter directly: no implicit
# sync, so the locally-installed codec survives both runs. -m perf is
# required: the repo's addopts (-m "default or default_async") would
# otherwise deselect the unmarked perf suite. The selector skips the JSON
# benchmarks (json_util is pure Python and identical for both codecs).
TEST_SELECTOR='(Flat or Deep or Full) and not Json'

# Run each codec's pass twice in swapped order (C→Rust, then Rust→C) and
# merge to the per-benchmark mean: machine state (turbo/thermal drift) moves
# over the ~6-minute window, so a single back-to-back C/Rust pair is
# systematically biased in favor of whichever codec runs first. The swap
# cancels that bias; the merged files are what gets compared and uploaded.
BSON_IMPL=c OUTPUT_FILE=results_c_a.json \
  .venv/bin/python -m pytest \
  test/performance/perf_test.py -m perf -k "$TEST_SELECTOR" -q

BSON_IMPL=rust OUTPUT_FILE=results_rust_a.json \
  .venv/bin/python -m pytest \
  test/performance/perf_test.py -m perf -k "$TEST_SELECTOR" -q

BSON_IMPL=rust OUTPUT_FILE=results_rust_b.json \
  .venv/bin/python -m pytest \
  test/performance/perf_test.py -m perf -k "$TEST_SELECTOR" -q

BSON_IMPL=c OUTPUT_FILE=results_c_b.json \
  .venv/bin/python -m pytest \
  test/performance/perf_test.py -m perf -k "$TEST_SELECTOR" -q

# Merge the swapped-order pairs into the two files the comparison reads.
.venv/bin/python - <<'PY'
import copy
import json


def merge(paths, out):
    values = {}
    for path in paths:
        for result in json.load(open(path)):
            name = result["info"]["test_name"]
            values.setdefault(name, []).append(result)
    merged = []
    for name, results in sorted(values.items()):
        # Carry the first run's entry through unchanged (info, args,
        # improvement_direction, measurement_unit — perf_test.py's schema)
        # and replace only the throughput value with the mean of the runs.
        # "type" keeps the per-run aggregation label for schema compatibility.
        entry = copy.deepcopy(results[0])
        idx = next(
            i for i, m in enumerate(entry["metrics"])
            if m["name"] == "megabytes_per_sec"
        )
        entry["metrics"][idx]["value"] = sum(
            r["metrics"][idx]["value"] for r in results
        ) / len(results)
        merged.append(entry)
    with open(out, "w") as fid:
        fid.write(json.dumps(merged, indent=4))


merge(["results_c_a.json", "results_c_b.json"], "results_c.json")
merge(["results_rust_a.json", "results_rust_b.json"], "results_rust.json")
PY
rm results_c_a.json results_c_b.json results_rust_a.json results_rust_b.json

# Print the comparison (also uploaded as a task artifact).
.venv/bin/python tools/compare_bson_perf.py results_c.json results_rust.json | tee bson_comparison.md

# Profile DeepDecoding once per codec for decode hotspot data. Entirely
# non-fatal: profiling is best-effort and the benchmark numbers never
# depend on it.
# The rhel90 perf image does not ship perf; try the platform package manager
# with passwordless sudo (the established pattern on Evergreen hosts, e.g.
# setup-system.sh) before skipping.
if ! command -v perf > /dev/null 2>&1; then
  if command -v dnf > /dev/null 2>&1; then
    sudo dnf install -y perf || sudo yum install -y perf || true
  elif command -v apt-get > /dev/null 2>&1; then
    sudo apt-get install -y -qq perf || true
  fi
fi
if command -v perf > /dev/null 2>&1; then
  # Lower the paranoid floor so user-space sampling works on hardened
  # images (kernel samples are dropped at >=2; user-space only is enough).
  sudo sysctl -w kernel.perf_event_paranoid=1 > /dev/null 2>&1 || true
  for impl in c rust; do
    echo "Profiling DeepDecoding (BSON_IMPL=$impl)..."
    perf record -F 199 --call-graph dwarf,16384 \
      -o "perf_deepdec_${impl}.data" \
      -- env BSON_IMPL="$impl" OUTPUT_FILE="results_profile_${impl}.json" \
      .venv/bin/python -m pytest test/performance/perf_test.py -m perf \
      -k 'DeepDecoding and not Json' -q \
      > /dev/null || echo "perf record failed (BSON_IMPL=$impl);" \
        "its profile may be missing or partial" >&2
    if [ -f "perf_deepdec_${impl}.data" ]; then
      # --no-children: self time is what distinguishes the codecs; the raw
      # perf.data is discarded after the report (binary and large).
      perf report --stdio --no-children --percent-limit 0.5 \
        -i "perf_deepdec_${impl}.data" > "perf_deepdec_${impl}.txt" || true
      rm -f "perf_deepdec_${impl}.data"
      # A failed report leaves an empty/partial file; don't upload one.
      [ -s "perf_deepdec_${impl}.txt" ] || rm -f "perf_deepdec_${impl}.txt"
    fi
    rm -f "results_profile_${impl}.json"
  done
elif command -v valgrind > /dev/null 2>&1; then
  # No perf (e.g. the rhel90 image and it cannot be installed there): fall
  # back to callgrind, which the ubuntu2204 image ships preinstalled.
  # Callgrind's instruction counts are exact and deterministic, so hotspot
  # attribution is valid at any run length — FASTBENCH keeps the ~30x
  # slowdown bounded, and pytest/interpreter startup churn is discountable
  # in the annotation (it lands in importlib/PyImport frames).
  # The native build emits AVX-512 (EVEX) instructions via memchr's runtime
  # dispatch, and valgrind 3.18 cannot decode EVEX — it SIGILLs mid-run.
  # Rebuild for the profiled run on x86-64-v3 (AVX2, VEX-encoded, which
  # valgrind handles — the C extension is baseline x86-64 and profiles
  # fine). Nothing after this block uses the codec.
  echo "Rebuilding the Rust codec without AVX-512 for valgrind..."
  RUSTFLAGS="-C target-cpu=x86-64-v3" .venv/bin/maturin develop --release \
    -m rust/bson/Cargo.toml
  for impl in c rust; do
    echo "Profiling DeepDecoding with callgrind (BSON_IMPL=$impl)..."
    FASTBENCH=1 BSON_IMPL="$impl" OUTPUT_FILE="results_profile_${impl}.json" \
      valgrind --tool=callgrind --callgrind-out-file="callgrind_${impl}.out" \
      .venv/bin/python -m pytest test/performance/perf_test.py -m perf \
      -k 'DeepDecoding and not Json' -q \
      || echo "callgrind run failed (BSON_IMPL=$impl); skipping its profile" >&2
    if [ -f "callgrind_${impl}.out" ]; then
      callgrind_annotate "callgrind_${impl}.out" > "perf_deepdec_${impl}.txt" || true
      rm -f "callgrind_${impl}.out"
      # A failed annotation leaves an empty/partial file; don't upload one.
      [ -s "perf_deepdec_${impl}.txt" ] || rm -f "perf_deepdec_${impl}.txt"
    fi
    rm -f "results_profile_${impl}.json"
  done
else
  echo "perf could not be installed and valgrind is missing; skipping the DeepDecoding profiles" >&2
fi
