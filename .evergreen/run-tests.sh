#!/bin/bash
# Run a test suite that was configured with setup-tests.sh.
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
SCRIPT_DIR="$( cd -- "$SCRIPT_DIR" > /dev/null 2>&1 && pwd )"
ROOT_DIR="$(dirname $SCRIPT_DIR)"

pushd $ROOT_DIR

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

# List the packages.
uv sync ${UV_ARGS} --reinstall --quiet
uv pip list

# Start the test runner.
uv run ${UV_ARGS} .evergreen/scripts/run_tests.py "$@"

popd
