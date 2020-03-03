#!/bin/bash

set -o xtrace
set -o errexit

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON_BINARY" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
else
    PYTHON="$PYTHON_BINARY"
fi

if $PYTHON -m virtualenv --version; then
    VIRTUALENV="$PYTHON -m virtualenv"
elif command -v virtualenv; then
    # We can remove this fallback after:
    # https://github.com/10gen/mongo-python-toolchain/issues/8
    VIRTUALENV="$(command -v virtualenv) -p $PYTHON"
else
    echo "Cannot test without virtualenv"
    exit 1
fi

$VIRTUALENV --never-download --no-wheel ocsptest
if [ "Windows_NT" = "$OS" ]; then
  . ocsptest/Scripts/activate
else
  . ocsptest/bin/activate
fi
trap "deactivate; rm -rf ocsptest" EXIT HUP

IS_PYTHON_2=$(python -c "import sys; sys.stdout.write('1' if sys.version_info < (3,) else '0')")
if [ $IS_PYTHON_2 = "1" ]; then
    echo "Using a Python 2"
    # Upgrade pip to install the cryptography wheel and not the tar.
    # <20.1 because 20.0.2 says a future release may drop support for 2.7.
    pip install --upgrade 'pip<20.1'
    # Upgrade setuptools because cryptography requires 18.5+.
    # <45 because 45.0 dropped support for 2.7.
    pip install --upgrade 'setuptools<45'
fi

pip install pyopenssl requests service_identity

OCSP_TLS_SHOULD_SUCCEED=${OCSP_TLS_SHOULD_SUCCEED} CA_FILE=${CA_FILE} python test/ocsp/test_ocsp.py
