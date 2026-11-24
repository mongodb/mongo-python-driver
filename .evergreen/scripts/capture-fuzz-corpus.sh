#!/bin/bash
# PYTHON-6178: capture coverage-increasing inputs from a fuzz run and open
# an automated PR growing fuzz/corpus/. Runs as the last command of the
# weekly "fuzz-capture" task, while the scratch corpus exists; manual
# `evergreen patch --variant fuzz-capture-rhel8 --task fuzz-capture` runs
# capture too.
set -eu

# Same environment as just.sh: venv on PATH for a modern Python.
. "$(dirname "${BASH_SOURCE:-$0}")/setup-dev-env.sh"

# No new units means no PR, and the github secrets flow is never touched.
uv run --no-sync python fuzz/capture_corpus.py
if [ -z "$(ls -A fuzz-scratch/capture 2>/dev/null || true)" ]; then
    echo "No new fuzz corpus units; nothing to do."
    exit 0
fi

# The replay runs the atheris targets, so it must request the fuzz group.
replay=(fuzz-scratch/capture/*)
for target in fuzz_decode fuzz_decode_all fuzz_is_valid; do
    echo "Replaying captured units against ${target}..."
    if ! uv run --group fuzz python "fuzz/${target}.py" "${replay[@]}" \
            -artifact_prefix="$PWD/fuzz-scratch/" -atheris_runs="${#replay[@]}"
    then
        echo "Captured unit fails ${target}; no corpus PR." >&2
        exit 1
    fi
done

bash .evergreen/scripts/create-fuzz-corpus-pr.sh \
    fuzz-scratch/capture fuzz-scratch/capture-summary.md
