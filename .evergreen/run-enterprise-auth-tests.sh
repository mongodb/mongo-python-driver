#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

echo "Running enterprise authentication tests"

export DB_USER="bob"
export DB_PASSWORD="pwd123"
if [ "Windows_NT" = "$OS" ]; then
    echo "Setting GSSAPI_PASS"
    export GSSAPI_PASS=${SASL_PASS}
    export GSSAPI_CANONICALIZE="true"
else
    # BUILD-3830
    touch ${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty
    export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

    echo "Writing keytab"
    echo ${KEYTAB_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab
    echo "Running kinit"
    kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p ${PRINCIPAL}
fi
echo "Setting GSSAPI variables"
export GSSAPI_HOST=${SASL_HOST}
export GSSAPI_PORT=${SASL_PORT}
export GSSAPI_PRINCIPAL=${PRINCIPAL}


echo "Running tests"
${PYTHON_BINARY} setup.py clean
${PYTHON_BINARY} setup.py test --xunit-output=xunit-results
