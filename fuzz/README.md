# BSON fuzz tests

Coverage-guided fuzz tests for the `bson` package, built on
[atheris](https://github.com/google/atheris) (PYTHON-4232).

Each target runs the C extension and the pure-Python implementation and
compares their results when both are present.

## Fuzz targets

| File | What it tests |
|---|---|
| `fuzz_decode.py` | `bson.decode()` + round-trip `encode()` |
| `fuzz_decode_all.py` | `bson.decode_all()` + per-document `encode()` |
| `fuzz_is_valid.py` | `bson.is_valid()` and the invariant that `is_valid() == True` implies `decode()` succeeds |

## Running

Atheris is a `uv` dependency group. Run targets from the repository root:

```bash
uv run --group fuzz python fuzz/fuzz_decode.py fuzz/regression/*
```

### Regression inputs (deterministic)

`regression/` holds committed reproducers for known bugs. Running the targets
over them runs on Evergreen as the `fuzz-regression` sub-test on every PR:

```bash
just setup-tests fuzz regression
just run-tests
```

Or without the justfile:

```bash
uv run --group fuzz python fuzz/fuzz_decode.py fuzz/regression/* -atheris_runs=3
uv run --group fuzz python fuzz/fuzz_decode_all.py fuzz/regression/* -atheris_runs=3
uv run --group fuzz python fuzz/fuzz_is_valid.py fuzz/regression/* -atheris_runs=3
```

When a target finds a bug, create a new ticket, add the reproducer as a new
file in `fuzz/regression/`, and fix the bug in the same PR.

### Seed corpus (discovery fuzzing)

The seed corpus is generated at build time from the vendored BSON spec corpus
(`../test/bson_corpus`), mirroring the Go driver's build-oss-fuzz-corpus:

```bash
uv run python fuzz/build_corpus.py /tmp/fuzz-seed.zip --data-dir test/bson_corpus
uv run python -m zipfile -e /tmp/fuzz-seed.zip /tmp/fuzz-corpus
```

The builder also synthesizes seeds the spec corpus lacks: boundary-value
documents and a ~500 KB stress document. Without a large seed, libFuzzer's
default mutation cap of 4096 bytes would keep the nightly away from
long-string and many-field code paths.

Run the targets against the corpus. Time-boxed runs suit CI; this runs on
Evergreen as the `fuzz` sub-test, daily:

```bash
just setup-tests fuzz fuzz
FUZZ_MAX_TIME=300 just run-tests
```

Or without the justfile:

```bash
uv run --group fuzz python fuzz/fuzz_decode.py /tmp/fuzz-corpus -max_total_time=60
```

Pass any [libFuzzer flags](https://llvm.org/docs/LibFuzzer.html#options)
after the corpus directory. The `Failed to find function __sanitizer_*`
startup warnings are benign libFuzzer symbol probes on uninstrumented runs;
native ASan-instrumented fuzzing is tracked separately (PYTHON-6169).

## Coverage report

Measure which lines the seed corpus reaches:

```bash
uv run --group fuzz --group coverage python -m coverage run fuzz/fuzz_decode.py /tmp/fuzz-corpus/* \
    -atheris_runs=$(( $(ls /tmp/fuzz-corpus | wc -l) ))
uv run --group coverage python -m coverage html
```

## Files

```
build_corpus.py     Seed corpus builder (mirrors the Go driver's build-oss-fuzz-corpus)
fuzz_decode.py      Fuzz target: bson.decode()
fuzz_decode_all.py  Fuzz target: bson.decode_all()
fuzz_is_valid.py    Fuzz target: bson.is_valid()
regression/         Known bug reproducers (committed; no corpus setup required)
```
