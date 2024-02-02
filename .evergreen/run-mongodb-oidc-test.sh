#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

echo "Running MONGODB-OIDC authentication tests"

OIDC_PROVIDER_NAME=${OIDC_PROVIDER_NAME:-"aws"}

if [ $OIDC_PROVIDER_NAME == "aws" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi

    # Get the drivers secrets.  Use an existing secrets file first.
    if [ ! -f "./secrets-export.sh" ]; then
        bash ${DRIVERS_TOOLS}/.evergreen/auth_aws/setup_secrets.sh drivers/oidc
    fi
    source ./secrets-export.sh

    # # If the file did not have our creds, get them from the vault.
    if [ -z "$OIDC_ATLAS_URI_SINGLE" ]; then
        bash ${DRIVERS_TOOLS}/.evergreen/auth_aws/setup_secrets.sh drivers/oidc
        source ./secrets-export.sh
    fi

    # Make the OIDC tokens.
    set -x
    pushd ${DRIVERS_TOOLS}/.evergreen/auth_oidc
    . ./oidc_get_tokens.sh
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
    export AWS_WEB_IDENTITY_TOKEN_FILE="$OIDC_TOKEN_DIR/test_user1"
    export OIDC_ADMIN_USER=$OIDC_ALTAS_USER
    export OIDC_ADMIN_PWD=$OIDC_ATLAS_PASSWORD

elif [ $OIDC_PROVIDER_NAME == "azure" ]; then
    if [ -z "${AZUREOIDC_AUDIENCE}" ]; then
        echo "Must specify an AZUREOIDC_AUDIENCE"
        exit 1
    fi
    set +x   # turn off xtrace for this portion
    export OIDC_ADMIN_USER=$AZUREOIDC_USERNAME
    export OIDC_ADMIN_PWD=pwd123
    set -x
    export MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
    MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE}&authMechanismProperties=PROVIDER_NAME:azure"
    export MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE},TOKEN_AUDIENCE:${AZUREOIDC_AUDIENCE}"
    export MONGODB_URI_MULTI=$MONGODB_URI_SINGLE
else
    echo "Unrecognized OIDC_PROVIDER_NAME $OIDC_PROVIDER_NAME"
    exit 1
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"
bash ./.evergreen/tox.sh -m test-eg -- "${@:1}"
