#!/bin/bash

set +x          # Do not print outputs
set -o errexit  # Exit the script with error if any of the commands fail

echo "Running MONGODB-OIDC authentication tests"
# ensure no secrets are printed in log files
set +x

# Make sure DRIVERS_TOOLS is set.
if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Must specify DRIVERS_TOOLS"
    exit 1
fi

# Get the drivers secrets.
bash .evergreen/tox.sh -m aws-secrets -- drivers/oidc
source ./secrets-export.sh

# Make the OIDC tokens.
pushd ${DRIVERS_TOOLS}/.evergreen/auth_oidc
export OIDC_TOKEN_DIR=/tmp/tokens
. ./activate-authoidcvenv.sh
python oidc_make_tokens.py
popd

# Set up variables and run the test.
export MONGODB_URI="$OIDC_URI_SINGLE"
export MONGODB_URI_MULTI="$OIDC_URI_MULTI"
export TEST_AUTH_OIDC=1
export AUTH="auth"
bash ./.evergreen/tox.sh -m test-eg
