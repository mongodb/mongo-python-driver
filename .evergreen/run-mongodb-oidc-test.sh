#!/bin/bash
# Script run on a remote host to test MONGODB-OIDC.
set -eu

echo "Running MONGODB-OIDC authentication tests on ${OIDC_ENV}..."

if [ ${OIDC_ENV} == "k8s" ]; then
    SUB_TEST_NAME=$K8S_VARIANT-remote
else
    SUB_TEST_NAME=$OIDC_ENV-remote
    sudo apt-get install -y python3-dev build-essential
fi

bash ./.evergreen/just.sh setup-tests auth_oidc $SUB_TEST_NAME
bash ./.evergreen/just.sh run-tests "${@:1}"

echo "Running MONGODB-OIDC authentication tests on ${OIDC_ENV}... done."
