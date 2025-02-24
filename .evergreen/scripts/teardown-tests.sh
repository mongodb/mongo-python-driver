#!/bin/bash
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
SCRIPT_DIR="$( cd -- "$SCRIPT_DIR" > /dev/null 2>&1 && pwd )"
ROOT_DIR="$(dirname $SCRIPT_DIR)"

pushd $ROOT_DIR > /dev/null

# Try to source the env file.
if [ -f $SCRIPT_DIR/scripts/env.sh ]; then
  echo "Sourcing env inputs"
  . $SCRIPT_DIR/env.sh
else
  echo "Not sourcing env inputs"
fi

# Handle test inputs.
if [ -f $SCRIPT_DIR/scripts/test-env.sh ]; then
  echo "Sourcing test inputs"
  . $SCRIPT_DIR/test-env.sh
else
  echo "Missing test inputs, please run 'just setup-test'"
fi

# Start the test runner.
uv run $SCRIPT_DIR/teardown_tests.py

popd > /dev/null
