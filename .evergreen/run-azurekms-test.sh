#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

source ${DRIVERS_TOOLS}/.evergreen/csfle/azurekms/secrets-export.sh
echo "Copying files ... begin"
export AZUREKMS_RESOURCEGROUP=${AZUREKMS_RESOURCEGROUP}
export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
tar czf /tmp/mongo-python-driver.tgz .
# shellcheck disable=SC2088
AZUREKMS_SRC="/tmp/mongo-python-driver.tgz" AZUREKMS_DST="~/" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/copy-file.sh
echo "Copying files ... end"
echo "Untarring file ... begin"
AZUREKMS_CMD="tar xf mongo-python-driver.tgz" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
echo "Untarring file ... end"
echo "Running test ... begin"
AZUREKMS_CMD="KEY_NAME=\"$AZUREKMS_KEYNAME\" KEY_VAULT_ENDPOINT=\"$AZUREKMS_KEYVAULTENDPOINT\" LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian10/master/latest/libmongocrypt.tar.gz SUCCESS=true TEST_FLE_AZURE_AUTO=1 ./.evergreen/tox.sh -m test-eg" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
echo "Running test ... end"
