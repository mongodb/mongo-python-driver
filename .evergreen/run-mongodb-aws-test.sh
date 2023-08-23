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
$PYTHON_BINARY -c "import os;print('ENV VARS HERE');print(sorted([(k, v[:2]) for k, v in os.environ.items()]))"
# ensure no secrets are printed in log files
set +x

if [ -z "${SKIP_PREPARE_AWS_ENV}" ]; then
  [ -s "${DRIVERS_TOOLS}/.evergreen/auth_aws/prepare_aws_env.sh" ] && source "${DRIVERS_TOOLS}/.evergreen/auth_aws/prepare_aws_env.sh"
fi

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

if [ -z "$PYTHON_BINARY" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

export TEST_AUTH_AWS=1
export AUTH="auth"
export SET_XTRACE_ON=1
bash ./.evergreen/tox.sh -m test-eg
