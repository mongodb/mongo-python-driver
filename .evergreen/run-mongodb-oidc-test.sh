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

MONGODB_URI=${MONGODB_URI:-"mongodb://localhost:27017"}
MONGODB_URI="${MONGODB_URI}/test?authMechanism=MONGODB-OIDC&directConnection=true&authMechanismProperties=DEVICE_NAME:aws"
#MONGODB_URI="${MONGODB_URI}/test?authMechanism=MONGODB-OIDC"

if [ "$USE_MULTIPLE_PRINCIPALS" = "true" ]; then
    MONGODB_URI="${MONGODB_URI}&authMechanismProperties=PRINCIPAL_NAME:717cc021e105be9843cd2005e5a4607beae5a4960ef8098cb1247481626090f8"
fi

if [ -z "${AWS_TOKEN_DIR}" ]; then
    echo "Must specify AWS_TOKEN_DIR"
    exit 1
fi

export MONGODB_URI="$MONGODB_URI"

echo $MONGODB_URI

if [ "$ASSERT_NO_URI_CREDS" = "true" ]; then
    if echo "$MONGODB_URI" | grep -q "@"; then
        echo "MONGODB_URI unexpectedly contains user credentials!";
        exit 1
    fi
fi

# show test output
set -x

# Workaround macOS python 3.9 incompatibility with system virtualenv.
if [ "$(uname -s)" = "Darwin" ]; then
    # TODO: change back to 3.9 before merging.
    VIRTUALENV="/Library/Frameworks/Python.framework/Versions/3.10/bin/python3 -m virtualenv"
else
    VIRTUALENV=$(command -v virtualenv)
fi

authtest () {
    if [ "Windows_NT" = "$OS" ]; then
      PYTHON=$(cygpath -m $PYTHON)
    fi

    echo "Running MONGODB-OIDC authentication tests with $PYTHON"
    $PYTHON --version

    $VIRTUALENV -p $PYTHON --never-download venvoidc
    if [ "Windows_NT" = "$OS" ]; then
      . venvoidc/Scripts/activate
    else
      . venvoidc/bin/activate
    fi
    python -m pip install '.[aws]'
    python test/auth_aws/test_auth_oidc.py -v
    deactivate
    rm -rf venvoidc
}

PYTHON=${PYTHON_BINARY:-}
if [ -z "$PYTHON" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

authtest
