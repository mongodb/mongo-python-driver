#!/bin/bash

set +x          # Disable debug trace
set -eu

echo "Running MONGODB-OIDC authentication tests on ${OIDC_ENV}..."

bash ./.evergreen/just.sh setup-tests auth_oidc $K8S_VARIANT-remote
bash ./.evergreen/just.sh run-tests "${@:1}"

echo "Running MONGODB-OIDC authentication tests on ${OIDC_ENV}... done."
