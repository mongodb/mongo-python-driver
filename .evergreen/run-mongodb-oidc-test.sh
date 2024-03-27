#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV=${OIDC_ENV:-"test"}

if [ $OIDC_ENV == "test" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi

    # Get the drivers secrets.  Use an existing secrets file first.
    if [ ! -f "${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh" ]; then
        . ${DRIVERS_TOOLS}/.evergreen/auth_oidc/setup-secrets.sh
    else
        source "${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh"
    fi

    # Make the OIDC tokens.
    set -x
    pushd ${DRIVERS_TOOLS}/.evergreen/auth_oidc
    . ./oidc_get_tokens.sh
    popd

    # Set up variables and run the test.
    if [ -n "${LOCAL_OIDC_SERVER:-}" ]; then
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
    export OIDC_TOKEN_FILE="$OIDC_TOKEN_DIR/test_user1"
    set +x   # turn off xtrace for this portion
    export OIDC_ADMIN_USER=$OIDC_ATLAS_USER
    export OIDC_ADMIN_PWD=$OIDC_ATLAS_PASSWORD
    set -x

elif [ $OIDC_ENV == "azure" ]; then
    if [ -z "${AZUREOIDC_RESOURCE:-}" ]; then
        echo "Must specify an AZUREOIDC_RESOURCE"
        exit 1
    fi
    set +x   # turn off xtrace for this portion
    export OIDC_ADMIN_USER=$AZUREOIDC_USERNAME
    export OIDC_ADMIN_PWD=pwd123
    set -x
    export MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
    MONGODB_URI_SINGLE="${MONGODB_URI}/?authMechanism=MONGODB-OIDC"
    MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE}&authMechanismProperties=ENVIRONMENT:azure"
    export MONGODB_URI_SINGLE="${MONGODB_URI_SINGLE},TOKEN_RESOURCE:${AZUREOIDC_RESOURCE}"
    export MONGODB_URI_MULTI=$MONGODB_URI_SINGLE
else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"
bash ./.evergreen/tox.sh -m test-eg -- "${@:1}"
