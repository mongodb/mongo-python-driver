#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
HERE=$(dirname ${BASH_SOURCE:-$0})
source ${DRIVERS_TOOLS}/.evergreen/csfle/azurekms/secrets-export.sh
echo "Copying files ... begin"
export AZUREKMS_RESOURCEGROUP=${AZUREKMS_RESOURCEGROUP}
export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SUCCESS=true TEST_FLE_AZURE_AUTO=1 LIBMONGOCRYPT_URL=$LIBMONGOCRYPT_URL bash $HERE/scripts/setup-test.sh
# Set up the remote files to test.
git add .
git commit -m "add files" || true
git archive -o /tmp/mongo-python-driver.tar --add-file $HERE/scripts/test-env.sh HEAD
tar -rf /tmp/mongo-python-driver.tar libmongocrypt
tar -rf /tmp/mongo-python-driver.tar
gzip -f /tmp/mongo-python-driver.tar
# shellcheck disable=SC2088
AZUREKMS_SRC="/tmp/mongo-python-driver.tar.gz" AZUREKMS_DST="~/" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/copy-file.sh
echo "Copying files ... end"
echo "Untarring file ... begin"
AZUREKMS_CMD="tar xf mongo-python-driver.tar.gz" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
echo "Untarring file ... end"
echo "Running test ... begin"
AZUREKMS_CMD="KEY_NAME=\"$AZUREKMS_KEYNAME\" KEY_VAULT_ENDPOINT=\"$AZUREKMS_KEYVAULTENDPOINT\" bash ./.evergreen/just.sh test-eg" \
    $DRIVERS_TOOLS/.evergreen/csfle/azurekms/run-command.sh
echo "Running test ... end"
bash $HERE/scripts/teardown-tests.sh
