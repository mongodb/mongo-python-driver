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

cleanup_tests() {
  # Avoid leaving the lock file in a changed state when we change the resolution type.
  if [ -n "${TEST_MIN_DEPS:-}" ]; then
    git checkout uv.lock || true
  fi
  cd $PREV_DIR
}

trap "cleanup_tests" SIGINT ERR

# Start the test runner.
echo "Running tests with UV_PYTHON=${UV_PYTHON:-}..."
uv run ${UV_ARGS} --reinstall-package pymongo .evergreen/scripts/run_tests.py "$@"
echo "Running tests with UV_PYTHON=${UV_PYTHON:-}... done."

cleanup_tests
