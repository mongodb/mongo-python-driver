#!/bin/bash

set -o xtrace   # Trace outputs.
set -o errexit  # Exit the script with error if any of the commands fail

echo "Running MONGODB-OIDC authentication tests"

# Make sure DRIVERS_TOOLS is set.
if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Must specify DRIVERS_TOOLS"
    exit 1
fi

# Get the drivers secrets.  Use an existing secrets file first.
if [ ! -f "./secrets-export.sh" ]; then
    bash .evergreen/tox.sh -m aws-secrets -- drivers/oidc
fi
source ./secrets-export.sh

# # If the file did not have our creds, get them from the vault.
if [ -z "$OIDC_ATLAS_URI_SINGLE" ]; then
    bash .evergreen/tox.sh -m aws-secrets -- drivers/oidc
    source ./secrets-export.sh
fi

# Make the OIDC tokens.
set -x
export OIDC_TOKEN_DIR=/tmp/tokens
mkdir -p $OIDC_TOKEN_DIR
pushd ${DRIVERS_TOOLS}/.evergreen/auth_oidc

. ./activate-authoidcvenv.sh
python oidc_get_tokens.py
ls $OIDC_TOKEN_DIR
popd

# Set up variables and run the test.
if [ -n "$LOCAL_OIDC_SERVER" ]; then
    export MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
    export MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    export MONGODB_URI_MULTI="${MONGODB_URI}:27018/?authMechanism=MONGODB-OIDC&directConnection=true"
else
    set +x   # turn off xtrace for this portion
    export MONGODB_URI="$OIDC_ATLAS_URI_SINGLE"
    export MONGODB_URI_SINGLE="$OIDC_ATLAS_URI_SINGLE/?authMechanism=MONGODB-OIDC"
    export MONGODB_URI_MULTI="$OIDC_ATLAS_URI_MULTI/?authMechanism=MONGODB-OIDC"
    set -x
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"
bash ./.evergreen/tox.sh -m test-eg
