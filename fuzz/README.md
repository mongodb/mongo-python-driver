# BSON fuzz tests

Coverage-guided fuzz tests for the `bson` package, built on
[atheris](https://github.com/google/atheris) (PYTHON-4232).

"Coverage-guided" means the fuzzer records which code paths each input
reaches and steers mutations toward untested code; it does not enumerate
the API surface.

Each target runs the C extension and the pure-Python implementation and
compares their results when both are present.

## Fuzz targets

| File | What it tests |
|---|---|
| `fuzz_decode.py` | round-trip `bson.decode()` + `encode()` |
| `fuzz_decode_all.py` | `bson.decode_all()` + per-document `encode()` |
| `fuzz_is_valid.py` | `bson.is_valid()` and the invariant that `is_valid() == True` implies `decode()` succeeds |

## Running

Atheris is a `uv` dependency group. Run targets from the repository root:

```bash
uv run --group fuzz python fuzz/fuzz_decode.py fuzz/regression/*
```

Atheris publishes CPython 3.12 to 3.14 wheels for Linux x86_64 only, so the
`fuzz` group cannot install on other interpreters or platforms. Pin a
compatible interpreter where one is available, and run in a Linux x86_64
container otherwise:

```bash
uv run --group fuzz --python 3.13 python fuzz/fuzz_decode.py fuzz/regression/*
```

The Evergreen fuzz tasks pin Linux CPython 3.13 and are unaffected.

### Regression inputs (deterministic)

`regression/` holds reproducers for known bugs. Running the targets
over them runs on Evergreen as the `fuzz-regression` sub-test on every PR:

```bash
just setup-tests fuzz regression
just run-tests
```

On every PR, the `fuzz-regression` task runs the committed reproducers and the full generated seed
corpus.

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

### Harvested corpus (nightly)

The nightly run seeds from the spec seed corpus plus every unit in
`corpus/`, and libFuzzer writes each coverage-increasing ("interesting")
input it discovers back into its corpus directory. After the targets
finish, the `fuzz` task harvests those new units, replays them through
every target once so a unit that fails any target is never published (it
stays in `fuzz-scratch/` for triage), and opens an automated
"[Fuzz Corpus]" PR against main (PYTHON-6178). Later nights
build on prior discoveries instead of restarting from the spec seeds.

Growth is bounded by per-unit and per-run caps, overridable on Evergreen:

- `FUZZ_CORPUS_MAX_UNIT_BYTES` (default 65536): skip units larger than this.
- `FUZZ_CORPUS_MAX_UNITS` (default 50): keep at most this many new units
  per run, smallest first.

Units over a cap are discarded, not deferred: they are never persisted, so
the fuzzer may re-derive them on later nights. A run that finds nothing new
opens no PR. The harvest deduplicates against `main` only, so while a
`[Fuzz Corpus]` PR is open, later nights can re-harvest its still-pending
units and the PRs overlap until one merges (digest-named files keep that
conflict-free). Crashing inputs are excluded:
they land in `fuzz-scratch/` as `crash-*` artifacts and follow the
`regression/` triage flow above. Corpus entries are fuzzer-generated
inputs, not code: skim the PR for size and for inputs that look like a
bug rather than a plain coverage gain before merging.

## Coverage report

Measure which lines the seed corpus reaches:

```bash
uv run --group fuzz --group coverage python -m coverage run fuzz/fuzz_decode.py /tmp/fuzz-corpus/* \
    -atheris_runs=$(( $(ls /tmp/fuzz-corpus | wc -l) ))
uv run --group coverage python -m coverage html
```

The run count equals the file count: replaying individual files (not a corpus
directory) performs no empty-input warmup, and letting the budget reach zero
gives atheris a graceful exit so coverage.py can flush its report.

## Files

```
build_corpus.py     Seed corpus builder (mirrors the Go driver's build-oss-fuzz-corpus)
harvest_corpus.py   Nightly harvest of new coverage-increasing inputs (PYTHON-6178)
fuzz_decode.py      Fuzz target: bson.decode()
fuzz_decode_all.py  Fuzz target: bson.decode_all()
fuzz_is_valid.py    Fuzz target: bson.is_valid()
fuzz_shared.py      Code shared by the fuzz targets
corpus/             Harvested coverage-increasing inputs; grown by automated nightly PRs
regression/         Known bug reproducers; run with the seed corpus on PRs
```
