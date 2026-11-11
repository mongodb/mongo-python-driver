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

bash .evergreen/scripts/create-fuzz-corpus-pr.sh \
    fuzz-scratch/harvest fuzz-scratch/harvest-summary.md
