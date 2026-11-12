#!/bin/bash
# Run a test suite that was configured with setup-tests.sh.
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
SCRIPT_DIR="$( cd -- "$SCRIPT_DIR" > /dev/null 2>&1 && pwd )"
ROOT_DIR="$(dirname $SCRIPT_DIR)"

PREV_DIR=$(pwd)
cd $ROOT_DIR

# Try to source the env file.
if [ -f $SCRIPT_DIR/scripts/env.sh ]; then
  echo "Sourcing env inputs"
  . $SCRIPT_DIR/scripts/env.sh
else
  echo "Not sourcing env inputs"
fi

# Handle test inputs.
if [ -f $SCRIPT_DIR/scripts/test-env.sh ]; then
  echo "Sourcing test inputs"
  . $SCRIPT_DIR/scripts/test-env.sh
else
  echo "Missing test inputs, please run 'just setup-tests'"
  exit 1
fi

# No-op on Evergreen (just.sh already ran this); needed for direct/local runs (PYTHON-6135).
. $SCRIPT_DIR/scripts/setup-uv-python.sh

# Start the test runner.
echo "Running tests with UV_PYTHON=${UV_PYTHON:-} UV_PYTHON_SEARCH_PATH=${UV_PYTHON_SEARCH_PATH:-}..."
echo "UV_ARGS=${UV_ARGS}"
uv run ${UV_ARGS} --reinstall-package pymongo .evergreen/scripts/run_tests.py "$@"
echo "Running tests with UV_PYTHON=${UV_PYTHON:-} UV_PYTHON_SEARCH_PATH=${UV_PYTHON_SEARCH_PATH:-}... done."

cd $PREV_DIR
