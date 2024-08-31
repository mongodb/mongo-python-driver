#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

# Check if we should run pytest directly.
if [ -n "$USE_PYTEST" ]; then
    ENV_NAME=testenv-$RANDOM
    createvirtualenv "$PYTHON_BINARY" $ENV_NAME
    # shellcheck disable=SC2064
    trap "deactivate; rm -rf $ENV_NAME" EXIT HUP
    python -m pip install -e ".[test]"
    pytest -v --capture=tee-sys --durations=5 --maxfail=10
    exit 0
fi


if $PYTHON_BINARY -m hatch --version; then
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
