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

# shellcheck disable=SC2154
if [ "${skip_EC2_auth_test:-}" = "true" ] && { [ "$1" = "ec2" ] || [ "$1" = "web-identity" ]; }; then
   echo "This platform does not support the EC2 auth test, skipping..."
   exit 0
fi

echo "Running MONGODB-AWS authentication tests for $1"

# Handle credentials and environment setup.
. "$DRIVERS_TOOLS"/.evergreen/auth_aws/aws_setup.sh "$1"

bash ./.evergreen/just.sh setup-tests auth_aws $1
bash ./.evergreen/just.sh run-tests
