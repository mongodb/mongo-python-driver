#!/bin/bash

# Don't trace to avoid secrets showing up in the logs
set -o errexit

echo "Running enterprise authentication tests"

export JAVA_HOME=/opt/java/jdk8

PYTHON_VERSION=$(${PYTHON_BINARY} -c 'import sys; sys.stdout.write(str(sys.version_info[0]))')
PLATFORM="$(${PYTHON_BINARY} -c 'import platform, sys; sys.stdout.write(platform.system())')"

export DB_USER="bob"
export DB_PASSWORD="pwd123"

# There is no kerberos package for Jython, but we do want to test PLAIN.
if [ ${PLATFORM} != "Java" ]; then
    # PyMongo 2.x doesn't support GSSAPI on Windows.
    if [ "Windows_NT" != "$OS" ]; then
        echo "Writing keytab"
        echo ${KEYTAB_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab
        echo "Running kinit"
        kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p ${PRINCIPAL}
        echo "Setting GSSAPI variables"
        export GSSAPI_HOST=${SASL_HOST}
        export GSSAPI_PORT=${SASL_PORT}
        export GSSAPI_PRINCIPAL=${PRINCIPAL}
    fi
    EXTRA_ARGS=""
else
    # Keep Jython 2.5 from running out of memory.
    EXTRA_ARGS="-J-XX:-UseGCOverheadLimit -J-Xmx4096m"
fi

# Set verbose test output flag.
if [ "$PYTHON_VERSION" = "3" ]; then
    # With Python 3, the tests do not accept a "--verbosity=2" flag.
    TEST_VERBOSITY="-v"
else
    # With Python 2, the tests accepts a "-v" flag but only "--verbosity=2"
    # causes the verbose output we want.
    TEST_VERBOSITY="--verbosity=2"
fi

echo "Running tests"
${PYTHON_BINARY} setup.py clean
${PYTHON_BINARY} $EXTRA_ARGS setup.py nosetests $TEST_VERBOSITY
