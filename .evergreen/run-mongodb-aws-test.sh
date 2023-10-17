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

# Handle credentials and environment setup.
. $DRIVERS_TOOLS/.evergreen/auth_aws/aws_setup.sh $1

# show test output
set -x

export TEST_AUTH_AWS=1
export AUTH="auth"
export SET_XTRACE_ON=1
bash ./.evergreen/tox.sh -m test-eg
