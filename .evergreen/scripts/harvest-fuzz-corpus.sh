#!/bin/bash
# PYTHON-6178: harvest coverage-increasing inputs from the nightly fuzz
# run and open an automated PR growing fuzz/corpus/. Runs as the last
# command of the Evergreen nightly "fuzz" task, after the targets have
# finished and while the scratch corpus still exists.
set -eu

# Never open corpus PRs from patch builds; the nightly variant only.
if [ "${is_patch:-}" = "true" ]; then
    echo "Patch build; skipping fuzz corpus harvest."
    exit 0
fi

# Same environment as just.sh: venv on PATH for a modern Python.
. "$(dirname "${BASH_SOURCE:-$0}")/setup-dev-env.sh"

# Harvest the new units. No new units means a quiet night: no commit, no
# PR (and the GitHub secrets flow is never touched).
uv run --no-sync python fuzz/harvest_corpus.py
if [ -z "$(ls -A fuzz-scratch/harvest 2>/dev/null || true)" ]; then
    echo "No new fuzz corpus units; nothing to do."
    exit 0
fi

# Replay the harvested units through every target before publishing.
# The targets ran sequentially over one shared corpus, so a unit found
# by a later target never ran through the earlier ones, and publishing
# it could break the regression run of every later PR. Each input runs
# exactly once, as in the regression sub-test; a replay failure leaves
# the unit in fuzz-scratch/ for triage and fails the task instead of
# opening a PR.
replay=(fuzz-scratch/harvest/*)
for target in fuzz_decode fuzz_decode_all fuzz_is_valid; do
    echo "Replaying harvested units against ${target}..."
    if ! uv run --no-sync python "fuzz/${target}.py" "${replay[@]}" \
            -artifact_prefix="$PWD/fuzz-scratch/" -atheris_runs="${#replay[@]}"
    then
        echo "Harvested unit fails ${target}; no corpus PR." >&2
        exit 1
    fi
done

bash .evergreen/scripts/create-fuzz-corpus-pr.sh \
    fuzz-scratch/harvest fuzz-scratch/harvest-summary.md
