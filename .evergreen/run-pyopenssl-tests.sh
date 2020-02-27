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
. pyopenssltest/bin/activate
trap "deactivate; rm -rf pyopenssltest" EXIT HUP

IS_PYTHON_2=$(python -c "import sys; sys.stdout.write('1' if sys.version_info < (3,) else '0')")
if [ $IS_PYTHON_2 = "1" ]; then
    echo "Using a Python 2"
    pip install --upgrade 'setuptools<45'
fi

pip install pyopenssl requests service_identity
python -c 'import sys; print(sys.version)'
python setup.py test
