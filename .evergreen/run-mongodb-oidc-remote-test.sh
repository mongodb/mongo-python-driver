#!/bin/bash

set +x          # Disable debug trace
set -eu

echo "Running MONGODB-OIDC remote tests"

OIDC_ENV=${OIDC_ENV:-"test"}

# Make sure DRIVERS_TOOLS is set.
if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Must specify DRIVERS_TOOLS"
    exit 1
fi

# Set up the remote files to test.
git add .
git commit -m "add files" || true
export TEST_TAR_FILE=/tmp/mongo-python-driver.tgz
git archive -o $TEST_TAR_FILE HEAD

pushd $DRIVERS_TOOLS

if [ $OIDC_ENV == "test" ]; then
    echo "Test OIDC environment does not support remote test!"
    exit 1

elif [ $OIDC_ENV == "azure" ]; then
    export AZUREOIDC_DRIVERS_TAR_FILE=$TEST_TAR_FILE
    export AZUREOIDC_TEST_CMD="OIDC_ENV=azure ./.evergreen/run-mongodb-oidc-test.sh"
    bash ./.evergreen/auth_oidc/azure/run-driver-test.sh

elif [ $OIDC_ENV == "gcp" ]; then
    export GCPOIDC_DRIVERS_TAR_FILE=$TEST_TAR_FILE
    export GCPOIDC_TEST_CMD="OIDC_ENV=gcp ./.evergreen/run-mongodb-oidc-test.sh"
    bash ./.evergreen/auth_oidc/gcp/run-driver-test.sh

elif [ $OIDC_ENV == "k8s" ]; then
    # Make sure K8S_VARIANT is set.
    if [ -z "$K8S_VARIANT" ]; then
        echo "Must specify K8S_VARIANT"
        popd
        exit 1
    fi

    bash ./.evergreen/auth_oidc/k8s/setup-pod.sh
    bash ./.evergreen/auth_oidc/k8s/run-self-test.sh
    export K8S_DRIVERS_TAR_FILE=$TEST_TAR_FILE
    export K8S_TEST_CMD="OIDC_ENV=k8s ./.evergreen/run-mongodb-oidc-test.sh"
    source ./.evergreen/auth_oidc/k8s/secrets-export.sh  # for MONGODB_URI
    bash ./.evergreen/auth_oidc/k8s/run-driver-test.sh
    bash ./.evergreen/auth_oidc/k8s/teardown-pod.sh

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    pod
    exit 1
fi

popd
