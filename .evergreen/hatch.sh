#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

if $PYTHON_BINARY -m hatch --version; then
    run_hatch() {
      $PYTHON_BINARY -m hatch run "$@"
    }
else # No toolchain hatch present, set up virtualenv before installing hatch
    createvirtualenv "$PYTHON_BINARY" hatchenv
    trap "deactivate; rm -rf hatchenv" EXIT HUP
    python -m pip install --prefer-binary -q hatch
    run_hatch() {
      python -m hatch run "$@"
    }
fi

run_hatch "${@:1}"
