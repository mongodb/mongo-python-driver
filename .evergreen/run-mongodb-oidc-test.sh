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

if [ -z "${OIDC_TOKEN_DIR}" ]; then
    echo "Must specify OIDC_TOKEN_DIR"
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

if [ -z "$PYTHON_BINARY" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

export OIDC_TEST=1
export SET_XTRACE_ON=1
bash ./.evergreen/tox.sh test-eg
