#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")

if [ -z "${TEST_CRYPT_SHARED:-}" ]; then
    export SKIP_CRYPT_SHARED=1
fi

# Override the tls files if applicable.
if [ "${SSL:-}" == "ssl" ]; then
    export TLS_CERT_KEY_FILE=${ROOT}/test/certificates/client.pem
    export TLS_PEM_KEY_FILE=${ROOT}/test/certificates/server.pem
    export TLS_CA_FILE=${ROOT}/test/certificates/ca.pem
fi

MONGODB_VERSION=${VERSION:-} \
    TOPOLOGY=${TOPOLOGY:-} \
    AUTH=${AUTH:-} \
    SSL=${SSL:-} \
    STORAGE_ENGINE=${STORAGE_ENGINE:-} \
    DISABLE_TEST_COMMANDS=${DISABLE_TEST_COMMANDS:-} \
    ORCHESTRATION_FILE=${ORCHESTRATION_FILE:-} \
    REQUIRE_API_VERSION=${REQUIRE_API_VERSION:-} \
    LOAD_BALANCER=${LOAD_BALANCER:-} \
    bash ${DRIVERS_TOOLS}/.evergreen/run-orchestration.sh
# run-orchestration generates expansion file with the MONGODB_URI for the cluster
