#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

# Supported/used environment variables:
#  MONGODB_URI    Set the URI, including an optional username/password to use
#                 to connect to the server via MONGODB-OIDC authentication
#                 mechanism.
#  PYTHON_BINARY  The Python version to use.

echo "Running MONGODB-OIDC authentication tests"
# ensure no secrets are printed in log files
set +x

# load the script
shopt -s expand_aliases # needed for `urlencode` alias
[ -s "${PROJECT_DIRECTORY}/prepare_mongodb_oidc.sh" ] && source "${PROJECT_DIRECTORY}/prepare_mongodb_oidc.sh"

PROVIDER_NAME=${PROVIDER_NAME:-"aws"}
export MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}

if [ "$PROVIDER_NAME" = "aws" ]; then
    export MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    export MONGODB_URI_MULTIPLE="${MONGODB_URI}:27018/?authMechanism=MONGODB-OIDC&directConnection=true"

    if [ -z "${OIDC_TOKEN_DIR}" ]; then
        echo "Must specify OIDC_TOKEN_DIR"
        exit 1
    fi
elif [ "$PROVIDER_NAME" = "azure" ]; then
    if [ -z "${AZUREOIDC_CLIENTID}" ]; then
        echo "Must specify an AZUREOIDC_CLIENTID"
        exit 1
    fi
    MONGODB_URI="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    MONGODB_URI="${MONGODB_URI}&authMechanismProperties=PROVIDER_NAME:azure"
    export MONGODB_URI="${MONGODB_URI},TOKEN_AUDIENCE:api%3A%2F%2F${AZUREOIDC_CLIENTID}"
fi

if [ "$ASSERT_NO_URI_CREDS" = "true" ]; then
    if echo "$MONGODB_URI" | grep -q "@"; then
        echo "MONGODB_URI unexpectedly contains user credentials!";
        exit 1
    fi
fi

# show test output
set -x
echo "MONGODB_URI=${MONGODB_URI}"

authtest () {
    if [ "Windows_NT" = "$OS" ]; then
      PYTHON=$(cygpath -m $PYTHON)
    fi

    echo "Running MONGODB-OIDC authentication tests with $PYTHON"
    $PYTHON --version

    $PYTHON -m venv venvoidc
    if [ "Windows_NT" = "$OS" ]; then
      . venvoidc/Scripts/activate
    else
      . venvoidc/bin/activate
    fi
    python -m pip install -U pip setuptools

    if [ "$PROVIDER_NAME" = "aws" ]; then
        python -m pip install '.[aws]'
        python test/auth_oidc/test_auth_oidc.py -v
    elif [ "$PROVIDER_NAME" = "azure" ]; then
        python -m pip install .
        python test/auth_oidc/test_auth_oidc_azure.py -v
    fi
    deactivate
    rm -rf venvoidc
    echo "MONGODB-OIDC authentication tests complete"
}

PYTHON=${PYTHON_BINARY:-$(which python3)}
if [ -z "$PYTHON" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

authtest
