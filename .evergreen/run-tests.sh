#!/bin/bash
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
ROOT_DIR="$(dirname $(dirname $SCRIPT_DIR))"

export PIP_QUIET=1  # Quiet by default
export PIP_PREFER_BINARY=1 # Prefer binary dists by default

# Try to source the env file.
if [ -f $SCRIPT_DIR/scripts/env.sh ]; then
  echo "Sourcing env inputs"
  source $SCRIPT_DIR/scripts/env.sh
else
  echo "Not sourcing env inputs"
fi

# Ensure there are test inputs.
if [ -f $SCRIPT_DIR/scripts/test-env.sh ]; then
  echo "Sourcing test inputs"
  source $SCRIPT_DIR/scripts/test-env.sh
else
  echo "Missing test inputs, please run 'just setup-test'"
fi

# Source the local secrets export file if available.
if [ -f "$ROOT_DIR/secrets-export.sh" ]; then
  source "$ROOT_DIR/secrets-export.sh"
fi

PYTHON_IMPL=$(uv run python -c "import platform; print(platform.python_implementation())")

# Ensure C extensions if applicable.
if [ -z "${NO_EXT:-}" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    uv run --frozen tools/fail_if_no_c.py
fi

if [ -n "${PYMONGOCRYPT_LIB:-}" ]; then
    # Ensure pymongocrypt is working properly.
    # shellcheck disable=SC2048
    uv run ${UV_ARGS} python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    # shellcheck disable=SC2048
    uv run ${UV_ARGS} python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by configure-env.sh for access to mongocryptd.
fi

PYTHON_IMPL=$(uv run python -c "import platform; print(platform.python_implementation())")
echo "Running ${AUTH:-noauth} tests over ${SSL:-nossl} with python $(uv python find)"
uv run python -c 'import sys; print(sys.version)'

# Show the installed packages
# shellcheck disable=SC2048
PIP_QUIET=0 uv run ${UV_ARGS} --with pip pip list

# Record the start time for a perf test.
if [ -n "${PERF_TEST:-}" ]; then
    start_time=$(date +%s)
fi

# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.
TEST_ARGS=${TEST_ARGS}
if [ "$#" -ne 0 ]; then
    TEST_ARGS=“$@”
fi
echo "Running tests with $TEST_ARGS..."
if [ -z "${GREEN_FRAMEWORK:-}" ]; then
    # shellcheck disable=SC2048
    uv run ${UV_ARGS} pytest $TEST_ARGS
else
    # shellcheck disable=SC2048
    uv run ${UV_ARGS} green_framework_test.py $GREEN_FRAMEWORK -v $TEST_ARGS
fi
echo "Running tests with $TEST_ARGS... done."

# Handle perf test post actions.
if [ -n "${PERF_TEST:-}" ]; then
    end_time=$(date +%s)
    elapsed_secs=$((end_time-start_time))

    cat results.json

    echo "{\"failures\": 0, \"results\": [{\"status\": \"pass\", \"exit_code\": 0, \"test_file\": \"BenchMarkTests\", \"start\": $start_time, \"end\": $end_time, \"elapsed\": $elapsed_secs}]}" > report.json

    cat report.json
fi

# Handle coverage post actions.
if [ -n "${COVERAGE:-}" ]; then
    rm -rf .pytest_cache
fi
