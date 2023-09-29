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

# Try to source exported AWS Secrets
if [ -f ./secrets-export.sh ]; then
  source ./secrets-export.sh
fi

if [ -n "$1" ]; then
  cd "${DRIVERS_TOOLS}"/.evergreen/auth_aws
  . ./activate-authawsvenv.sh
  python aws_tester.py "$1"
  cd -
fi

if [ -z "${SKIP_PREPARE_AWS_ENV}" ]; then
  [ -s  PYTHON_BINARY=python "${DRIVERS_TOOLS}/.evergreen/auth_aws/prepare_aws_env.sh" ] && source "${DRIVERS_TOOLS}/.evergreen/auth_aws/prepare_aws_env.sh"
fi

if [ -n "$USE_ENV_VAR_CREDS" ]; then
  export AWS_ACCESS_KEY_ID=$IAM_AUTH_ECS_ACCOUNT
  export AWS_SECRET_ACCESS_KEY=$IAM_AUTH_ECS_SECRET_ACCESS_KEY
fi

if [ -n "$USE_WEB_IDENTITY_CREDS" ]; then
  export AWS_ROLE_ARN=$IAM_AUTH_ASSUME_WEB_ROLE_NAME
  export AWS_WEB_IDENTITY_TOKEN_FILE=$IAM_WEB_IDENTITY_TOKEN_FILE
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

if [ -z "$PYTHON_BINARY" ]; then
    echo "Cannot test without specifying PYTHON_BINARY"
    exit 1
fi

# show test output
set -x

export TEST_AUTH_AWS=1
export AUTH="auth"
export SET_XTRACE_ON=1
bash ./.evergreen/tox.sh -m test-eg
