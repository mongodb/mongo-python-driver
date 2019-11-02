#!/bin/bash

set -o xtrace
set -o errexit

export DB_USER="bob"
export DB_PASSWORD="pwd123"
export CLIENT_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
export CA_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
else
    PYTHON="$PYTHON_BINARY"
fi

$PYTHON -m virtualenv pyopenssltest
trap "deactivate; rm -rf pyopenssltest" EXIT HUP
. pyopenssltest/bin/activate
pip install pyopenssl>=17.2.0 service_identity>=18.1.0
pip list
python -c 'import sys; print(sys.version)'
python setup.py test
