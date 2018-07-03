#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

export JAVA_HOME=/opt/java/jdk8

IMPL=$(${PYTHON_BINARY} -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
if [ $IMPL = "Jython" -o $IMPL = "PyPy" ]; then
    $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    PYTHON=python
else
    PYTHON=$PYTHON_BINARY
fi

echo "Running tests"
$PYTHON test/atlas/test_connection.py
