#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

if [ -z "$PYTHON_BINARY" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi

    . $DRIVERS_TOOLS/.evergreen/find-python3.sh
    PYTHON_BINARY="$(find_python3 2>/dev/null)" || exit 1
fi

TOX=$($PYTHON_BINARY -m tox --version 2>/dev/null || echo "")
if [ -n "$TOX" ]; then
    run_tox() {
      $PYTHON_BINARY -m tox "$@"
    }
else # No toolchain present, set up virtualenv before installing tox
    . .evergreen/utils.sh
    venvcreate "$PYTHON_BINARY" toxenv
    trap "deactivate; rm -rf toxenv" EXIT HUP
    python -m pip install -q tox
    run_tox() {
      python -m tox "$@"
    }
fi

run_tox "${@:1}"
