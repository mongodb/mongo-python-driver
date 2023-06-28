#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

PYTHON_BINARY="$1"

if $PYTHON_BINARY -m tox --version; then
    alias run_tox='$PYTHON_BINARY -m tox'
elif python3 -m tox --version; then
    alias run_tox='python3 -m tox'
elif tox --version; then
    alias run_tox=tox
else # No toolchain present, set up venv before installing tox
    $PYTHON_BINARY -m venv env
    if [ "Windows_NT" = "$OS" ]; then
        # Workaround https://bugs.python.org/issue32451:
        # mongovenv/Scripts/activate: line 3: $'\r': command not found
        dos2unix env/Scripts/activate || true
        . env/Scripts/activate
    else
        . env/bin/activate
    fi
    $PYTHON_BINARY -m pip install tox
    alias run_tox='$PYTHON_BINARY -m tox'
fi

alias python='$PYTHON_BINARY'
run_tox "${@:2}"

if command -v deactivate &> /dev/null; then
    deactivate
fi
