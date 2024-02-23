#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

. $DRIVERS_TOOLS/.evergreen/csfle/azurekms/setup-secrets.sh
PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3 \
    KEY_NAME="${AZUREKMS_KEYNAME}" \
    KEY_VAULT_ENDPOINT="${AZUREKMS_KEYVAULTENDPOINT}" \
    LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian10/master/latest/libmongocrypt.tar.gz \
    SUCCESS=false TEST_FLE_AZURE_AUTO=1 \
    ./.evergreen/tox.sh -m test-eg
