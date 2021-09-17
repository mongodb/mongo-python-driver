#!/bin/bash

# Exit on error and enable trace.
set -o errexit
set -o xtrace

export JAVA_HOME=/opt/java/jdk8

# Attempt to find system pip before creating a virtualenv
PIP=$(command -v pip2 || command -v pip)

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

if [ $IMPL = "Jython" ]; then
    echo "Using Jython"
    $PIP download certifi
    python -m pip install --no-index -f file://$(pwd) certifi
elif [ $IMPL = "PyPy" ]; then
    echo "Using PyPy"
    python -m pip install certifi
else
    IS_PRE_279=$(python -c "import sys; sys.stdout.write('1' if sys.version_info < (2, 7, 9) else '0')")
    if [ $IS_PRE_279 = "1" ]; then
        echo "Using a Pre-2.7.9 CPython"
        python -m pip install -r .evergreen/test-pyopenssl-requirements.txt
    else
        echo "Using CPython 2.7.9+"
    fi
fi

echo "Running tests without dnspython"
python test/atlas/test_connection.py

# dnspython is incompatible with Jython so don't test that combination.
if [ $IMPL != "Jython" ]; then
    python -m pip install dnspython
    echo "Running tests with dnspython"
    MUST_TEST_SRV="1" python test/atlas/test_connection.py
fi
