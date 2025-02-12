#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
HERE=$(dirname ${BASH_SOURCE:-$0})
. $DRIVERS_TOOLS/.evergreen/csfle/azurekms/setup-secrets.sh
export LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SUCCESS=false TEST_FLE_AZURE_AUTO=1 bash $HERE/scripts/setup-tests.sh
PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3 \
    KEY_NAME="${AZUREKMS_KEYNAME}" \
    KEY_VAULT_ENDPOINT="${AZUREKMS_KEYVAULTENDPOINT}" \
    $HERE/just.sh test-eg
bash $HERE/scripts/teardown-tests.sh
