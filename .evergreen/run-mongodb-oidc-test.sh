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

elif [ $OIDC_ENV == "k8s" ]; then
    echo "Running oidc on k8s"

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

COVERAGE=1 bash ./.evergreen/just.sh setup-tests auth_oidc
bash ./.evergreen/just.sh run-tests "${@:1}"
