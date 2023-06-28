#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

PYTHON_BINARY="$1"

. .evergreen/utils.sh

if $PYTHON_BINARY -m tox --version; then
    alias run_tox='$PYTHON_BINARY -m tox'
elif python3 -m tox --version; then
    alias run_tox='python3 -m tox'
elif tox --version; then
    alias run_tox=tox
else # No toolchain present, set up venv before installing tox
    createvirtualenv PYTHON_BINARY env
    alias run_tox='$PYTHON_BINARY -m tox'
fi

alias python='$PYTHON_BINARY'
run_tox "${@:2}"

if command -v deactivate &> /dev/null; then
    trap "deactivate; rm -rf env" EXIT HUP
fi
