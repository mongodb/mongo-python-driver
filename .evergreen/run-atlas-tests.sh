#!/bin/bash

# Exit on error and enable trace.
set -o errexit
set -o xtrace

export JAVA_HOME=/opt/java/jdk8

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON_BINARY=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON_BINARY" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
fi

IMPL=$(${PYTHON_BINARY} -c "import platform, sys; sys.stdout.write(platform.python_implementation())")

if [ $IMPL = "Jython" ]; then
    # The venv created by createvirtualenv is incompatible with Jython
    $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
    . atlastest/bin/activate
else
    # All other pythons work with createvirtualenv.
    . .evergreen/utils.sh
    createvirtualenv $PYTHON_BINARY atlastest
fi
trap "deactivate; rm -rf atlastest" EXIT HUP

if [ $IMPL = "Jython" -o $IMPL = "PyPy" ]; then
    echo "Using Jython or PyPy"
    python -m pip install certifi
else
    IS_PRE_279=$(python -c "import sys; sys.stdout.write('1' if sys.version_info < (2, 7, 9) else '0')")
    if [ $IS_PRE_279 = "1" ]; then
        echo "Using a Pre-2.7.9 CPython"
        python -m pip install pyopenssl>=17.2.0 service_identity>18.1.0
    else
        echo "Using CPython 2.7.9+"
    fi
fi

echo "Running tests without dnspython"
python test/atlas/test_connection.py

python -m pip install dnspython
echo "Running tests with dnspython"
PYMONGO_MUST_HAVE_DNS="1" python test/atlas/test_connection.py
