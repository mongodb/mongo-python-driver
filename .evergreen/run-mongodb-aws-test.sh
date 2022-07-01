#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

# Supported/used environment variables:
#  MONGODB_URI    Set the URI, including an optional username/password to use
#                 to connect to the server via MONGODB-AWS authentication
#                 mechanism.
#  PYTHON_BINARY  The Python version to use.

echo "Running MONGODB-AWS authentication tests"
# ensure no secrets are printed in log files
set +x

# load the script
shopt -s expand_aliases # needed for `urlencode` alias
[ -s "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh" ] && source "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh"

MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
MONGODB_URI="${MONGODB_URI}/aws?authMechanism=MONGODB-AWS"
if [[ -n ${SESSION_TOKEN} ]]; then
    MONGODB_URI="${MONGODB_URI}&authMechanismProperties=AWS_SESSION_TOKEN:${SESSION_TOKEN}"
fi

export MONGODB_URI="$MONGODB_URI"

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
    VIRTUALENV="/Library/Frameworks/Python.framework/Versions/3.9/bin/python3 -m virtualenv"
else
    VIRTUALENV=$(command -v virtualenv)
fi

authtest () {
    if [ "Windows_NT" = "$OS" ]; then
      PYTHON=$(cygpath -m $PYTHON)
    fi

    echo "Running MONGODB-AWS authentication tests with $PYTHON"
    $PYTHON --version

    $VIRTUALENV -p $PYTHON --never-download venvaws
    if [ "Windows_NT" = "$OS" ]; then
      . venvaws/Scripts/activate
    else
      . venvaws/bin/activate
    fi
    python -m pip install '.[aws]'
    git clone https://github.com/blink1073/pymongo-auth-aws.git
    pushd pymongo-auth-aws
    git fetch origin DRIVERS-2333-2
    git checkout DRIVERS-2333-2
    popd
    python test/auth_aws/test_auth_aws.py
    deactivate
    rm -rf venvaws
}

PYTHON=${PYTHON_BINARY:-}
if [ -z "$PYTHON" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

authtest
