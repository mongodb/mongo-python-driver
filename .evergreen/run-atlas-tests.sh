#!/bin/bash

# Exit on error and enable trace.
set -o errexit
set -o xtrace

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON_BINARY=$(command -v python3) || true
    if [ -z "$PYTHON_BINARY" ]; then
        echo "Cannot test without python3 installed!"
        exit 1
    fi
fi

. .evergreen/utils.sh
createvirtualenv $PYTHON_BINARY atlastest
trap "deactivate; rm -rf atlastest" EXIT HUP

echo "Running tests without dnspython"
python test/atlas/test_connection.py

python -m pip install dnspython
echo "Running tests with dnspython"
MUST_TEST_SRV="1" python test/atlas/test_connection.py
