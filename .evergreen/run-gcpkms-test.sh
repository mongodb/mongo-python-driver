#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
HERE=$(dirname ${BASH_SOURCE:-$0})

source ${DRIVERS_TOOLS}/.evergreen/csfle/gcpkms/secrets-export.sh
echo "Copying files ... begin"
export GCPKMS_GCLOUD=${GCPKMS_GCLOUD}
export GCPKMS_PROJECT=${GCPKMS_PROJECT}
export GCPKMS_ZONE=${GCPKMS_ZONE}
export GCPKMS_INSTANCENAME=${GCPKMS_INSTANCENAME}
export LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SKIP_SERVERS=1 bash $HERE/setup-encryption.sh
tar czf /tmp/mongo-python-driver.tgz .
GCPKMS_SRC=/tmp/mongo-python-driver.tgz GCPKMS_DST=$GCPKMS_INSTANCENAME: $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/copy-file.sh
echo "Copying files ... end"
echo "Untarring file ... begin"
GCPKMS_CMD="tar xf mongo-python-driver.tgz" $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/run-command.sh
echo "Untarring file ... end"
echo "Running test ... begin"
GCPKMS_CMD="SUCCESS=true TEST_FLE_GCP_AUTO=1 ./.evergreen/hatch.sh test:test-eg" $DRIVERS_TOOLS/.evergreen/csfle/gcpkms/run-command.sh
echo "Running test ... end"
bash $HERE/teardown-encryption.sh
