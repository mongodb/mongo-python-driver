#!/bin/bash

set -o xtrace
set -o errexit

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON_BINARY=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON_BINARY" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
fi

$PYTHON_BINARY -m virtualenv --never-download --no-wheel ocsptest
    . ocsptest/bin/activate
    trap "deactivate; rm -rf ocsptest" EXIT HUP
    pip install pyopenssl requests service_identity
    PYTHON=python

OCSP_TLS_SHOULD_SUCCEED=${OCSP_TLS_SHOULD_SUCCEED} CA_FILE=${CA_FILE} $PYTHON test/ocsp/test_ocsp.py
