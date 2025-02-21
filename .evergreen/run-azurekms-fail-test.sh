#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
HERE=$(dirname ${BASH_SOURCE:-$0})
. $DRIVERS_TOOLS/.evergreen/csfle/azurekms/setup-secrets.sh
bash $HERE/just.sh setup-test kms azure-fail
KEY_NAME="${AZUREKMS_KEYNAME}" \
    KEY_VAULT_ENDPOINT="${AZUREKMS_KEYVAULTENDPOINT}" \
    $HERE/just.sh test-eg
bash $HERE/scripts/teardown-tests.sh
