#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

. $DRIVERS_TOOLS/.evergreen/csfle/azurekms/setup-secrets.sh
LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SKIP_SERVERS=1 bash ./.evergreen/setup-encryption.sh
PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3 \
    KEY_NAME="${AZUREKMS_KEYNAME}" \
    KEY_VAULT_ENDPOINT="${AZUREKMS_KEYVAULTENDPOINT}" \
    SUCCESS=false TEST_FLE_AZURE_AUTO=1 \
    ./.evergreen/hatch.sh test:test-eg
bash ./evergreen/teardown-encryption.sh
