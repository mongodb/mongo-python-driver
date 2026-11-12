#!/bin/bash
# Tear down any services that were used by tests.
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $SCRIPT_DIR/env.sh ]; then
  echo "Sourcing env inputs"
  . $SCRIPT_DIR/env.sh
else
  echo "Not sourcing env inputs"
fi

# Handle test inputs.
if [ -f $SCRIPT_DIR/test-env.sh ]; then
  echo "Sourcing test inputs"
  . $SCRIPT_DIR/test-env.sh
else
  echo "Missing test inputs, please run 'just setup-tests'"
fi

# Teardown the test runner.
uv run $SCRIPT_DIR/teardown_tests.py
