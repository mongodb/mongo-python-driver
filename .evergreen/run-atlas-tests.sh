#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

export JAVA_HOME=/opt/java/jdk8

IMPL=$(${PYTHON_BINARY} -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
PYTHON_VERSION=$(${PYTHON_BINARY} -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
if [ $PYTHON_VERSION = "2.6" -o $IMPL = "Jython" ]; then
    # TODO - When Jython has its own virtualenv install use it instead.
    /opt/python/2.6/bin/virtualenv -p ${PYTHON_BINARY} --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    if [ $PYTHON_VERSION = "2.6" ]; then
        pip install unittest2
    fi
    PYTHON=python
elif [ $IMPL = "PyPy" -a $PYTHON_VERSION = "3.2" ]; then
    $PYTHON_BINARY -m virtualenv --never-download --no-wheel atlastest
    . atlastest/bin/activate
    trap "deactivate; rm -rf atlastest" EXIT HUP
    pip install certifi
    # Portable pypy3.2 can't load CA certs from the system.
    # https://github.com/squeaky-pl/portable-pypy/issues/15
    export SSL_CERT_FILE=$(python -c "import certifi; print(certifi.where())")
    PYTHON=python
else
    PYTHON=$PYTHON_BINARY
fi

echo "Running tests"
$PYTHON test/atlas/test_connection.py
