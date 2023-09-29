#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

if [ -z "$PYTHON_BINARY" ]; then
    # If DRIVERS_TOOLS is not set, use the current python3.
    if [ -z "$DRIVERS_TOOLS" ]; then
        PYTHON_BINARY=python3
    else
        . $DRIVERS_TOOLS/.evergreen/find-python3.sh
        PYTHON_BINARY="$(find_python3 2>/dev/null)" || exit 1
    fi
fi

TOX=$($PYTHON_BINARY -m tox --version 2>/dev/null || echo "")
if [ -n "$TOX" ]; then
    run_tox() {
      $PYTHON_BINARY -m tox "$@"
    }
else # No toolchain present, set up virtualenv before installing tox
    # If DRIVERS_TOOLS is set, we are on an EVG host, use a virtual env.
    # Otherwise we are on temporary host, we can install directly.
    if [ -n "$DRIVERS_TOOLS" ]; then
        . .evergreen/utils.sh
        venvcreate "$PYTHON_BINARY" toxenv
        trap "deactivate; rm -rf toxenv" EXIT HUP
    fi
    python -m pip install -q tox
    run_tox() {
      python -m tox "$@"
    }
fi

run_tox "${@:1}"
