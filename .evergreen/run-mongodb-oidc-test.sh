#!/bin/bash

set +x          # Disable debug trace
set -eu

echo "Running MONGODB-OIDC authentication tests"

if [ $OIDC_ENV == "azure" ]; then
    source ./env.sh

elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh

elif [ $OIDC_ENV == "k8s" ]; then
    echo "Running oidc on k8s"

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

bash ./.evergreen/just.sh setup-tests auth_oidc remote
bash ./.evergreen/just.sh run-tests "${@:1}"
