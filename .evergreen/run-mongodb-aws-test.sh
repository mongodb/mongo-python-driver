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

# show test output
set -x

VIRTUALENV=$(command -v virtualenv)

authtest () {
    if [ "Windows_NT" = "$OS" ]; then
      PYTHON=$(cygpath -m $PYTHON)
    fi

    echo "Running MONGODB-AWS authentication tests with $PYTHON"
    $PYTHON --version

    $VIRTUALENV -p $PYTHON --system-site-packages --never-download venvaws
    if [ "Windows_NT" = "$OS" ]; then
      . venvaws/Scripts/activate
    else
      . venvaws/bin/activate
    fi
    pip install requests botocore

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
