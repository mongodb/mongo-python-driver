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

MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
MONGODB_URI_MULTIPLE="${MONGODB_URI}:27018/?authMechanism=MONGODB-OIDC&directConnection=true"

if [ -z "${AWS_TOKEN_DIR}" ]; then
    echo "Must specify AWS_TOKEN_DIR"
    exit 1
fi

export MONGODB_URI_SINGLE="$MONGODB_URI_SINGLE"
export MONGODB_URI_MULTIPLE="$MONGODB_URI_MULTIPLE"
export MONGODB_URI="$MONGODB_URI"

echo $MONGODB_URI_SINGLE
echo $MONGODB_URI_MULTIPLE
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
