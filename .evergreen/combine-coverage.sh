#!/bin/bash
#
# Coverage combine merges (and removes) all the coverage files and
# generates a new .coverage file in the current directory.

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

createvirtualenv "$PYTHON_BINARY" covenv
pip install -q coverage

pip list
ls -la coverage/

python -m coverage combine coverage/coverage.*
python -m coverage html -d htmlcov
