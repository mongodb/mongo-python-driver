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
# Keep in sync with run-tests.sh
# coverage >=5 is needed for relative_files=true.
pip install -q "coverage>=5,<=7.5"

pip list
ls -la coverage/

python -m coverage combine coverage/coverage.*
python -m coverage html -d htmlcov
