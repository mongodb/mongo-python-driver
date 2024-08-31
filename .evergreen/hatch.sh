#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

# Check if we should skip hatch and run the tests directly.
if [ -n "$SKIP_HATCH" ]; then
    ENV_NAME=testenv-$RANDOM
    createvirtualenv "$PYTHON_BINARY" $ENV_NAME
    # shellcheck disable=SC2064
    trap "deactivate; rm -rf $ENV_NAME" EXIT HUP
    python -m pip install -e ".[test]"
    run_hatch() {
      bash ./.evergreen/run-tests.sh "$@"
    }
elif $PYTHON_BINARY -m hatch --version; then
    run_hatch() {
      $PYTHON_BINARY -m hatch run "$@"
    }
else # No toolchain hatch present, set up virtualenv before installing hatch
    # Use a random venv name because the encryption tasks run this script multiple times in the same run.
    ENV_NAME=hatchenv-$RANDOM
    createvirtualenv "$PYTHON_BINARY" $ENV_NAME
    # shellcheck disable=SC2064
    trap "deactivate; rm -rf $ENV_NAME" EXIT HUP
    python -m pip install -q hatch
    run_hatch() {
      python -m hatch run "$@"
    }
fi

run_hatch "${@:1}"
