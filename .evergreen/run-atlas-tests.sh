#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

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
if [ $IMPL = "Jython" -o $IMPL = "PyPy" ]; then
    echo "Using Jython or PyPy"
    $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    PYTHON=python
else
    IS_PRE_279=$(${PYTHON_BINARY} -c "import sys; sys.stdout.write('1' if sys.version_info < (2, 7, 9) else '0')")
    if [ $IS_PRE_279 = "1" ]; then
        echo "Using a Pre-2.7.9 CPython"
        $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
        . atlastest/bin/activate
        trap "deactivate; rm -rf atlastest" EXIT HUP
        pip install pyopenssl>=17.2.0 service_identity>18.1.0
        PYTHON=python
    else
        echo "Using CPython 2.7.9+"
        PYTHON=$PYTHON_BINARY
    fi
fi

echo "Running tests"
$PYTHON test/atlas/test_connection.py
