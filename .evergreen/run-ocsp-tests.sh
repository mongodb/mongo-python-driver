#!/bin/bash

set -o xtrace
set -o errexit

# For createvirtualenv.
. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
else
    PYTHON="$PYTHON_BINARY"
fi

createvirtualenv $PYTHON ocsptest
trap "deactivate; rm -rf ocsptest" EXIT HUP

python -m pip install --prefer-binary pyopenssl requests service_identity

OCSP_TLS_SHOULD_SUCCEED=${OCSP_TLS_SHOULD_SUCCEED} CA_FILE=${CA_FILE} python test/ocsp/test_ocsp.py
