#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

export JAVA_HOME=/opt/java/jdk8

IMPL=$(${PYTHON_BINARY} -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
PYTHON_VERSION=$(${PYTHON_BINARY} -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
if [ $IMPL = "Jython" -o $IMPL = "PyPy" ]; then
    $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    if [ $PYTHON_VERSION = "3.2" ]; then
        # Portable pypy3.2 can't load CA certs from the system.
        # https://github.com/squeaky-pl/portable-pypy/issues/15
        export SSL_CERT_FILE=$(python -c "import certifi; print(certifi.where())")
    fi
    PYTHON=python
else
    PYTHON=$PYTHON_BINARY
fi

echo "Running tests"
$PYTHON test/atlas/test_connection.py
