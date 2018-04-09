#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

export JAVA_HOME=/opt/java/jdk8

PLATFORM=$(${PYTHON_BINARY} -c "import platform, sys; sys.stdout.write(platform.system())")
PYTHON_VERSION=$(${PYTHON_BINARY} -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
if [ $PYTHON_VERSION = "2.6" -o $PLATFORM = "Java" ]; then
    /opt/python/2.6/bin/virtualenv -p ${PYTHON_BINARY} --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    if [ $PYTHON_VERSION = "2.6" ]; then
        pip install unittest2
    fi
    PYTHON=python
else
    PYTHON=$PYTHON_BINARY
fi

echo "Running tests"
$PYTHON test/atlas/test_connection.py
