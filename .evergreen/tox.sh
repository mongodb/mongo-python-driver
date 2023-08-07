#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    # Use Python 3 from the server toolchain to test on ARM, POWER or zSeries if a
    # system python3 doesn't exist or exists but is older than 3.7.
    if is_python_37 "$(command -v python3)"; then
        PYTHON_BINARY=$(command -v python3)
    elif is_python_37 "$(command -v /opt/mongodbtoolchain/v3/bin/python3)"; then
        PYTHON_BINARY=$(command -v /opt/mongodbtoolchain/v3/bin/python3)
    else
        echo "Cannot test without python3.7+ installed!"
    fi
fi

if $PYTHON_BINARY -m tox --version; then
    run_tox() {
      $PYTHON_BINARY -m tox "$@"
    }
else # No toolchain present, set up virtualenv before installing tox
    createvirtualenv "$PYTHON_BINARY" toxenv
    trap "deactivate; rm -rf toxenv" EXIT HUP
    python -m pip install tox
    run_tox() {
      python -m tox "$@"
    }
fi

run_tox "${@:2}"
