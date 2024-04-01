#!/bin/bash

set +x          # Disable debug trace
set -eu

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV=${OIDC_ENV:-"test"}

if [ $OIDC_ENV == "test" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

elif [ $OIDC_ENV == "azure" ]; then
    source ./env.sh

elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

if [[ $OIDC_PROVIDER_NAME == "azure" ]] || [[ $OIDC_PROVIDER_NAME == "gcp" ]]; then
    export MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
    MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE}&authMechanismProperties=PROVIDER_NAME:${OIDC_PROVIDER_NAME}"
    export MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE},TOKEN_AUDIENCE:${OIDC_AUDIENCE}"
    export MONGODB_URI_MULTI=$MONGODB_URI_SINGLE
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"
bash ./.evergreen/tox.sh -m test-eg -- "${@:1}"
