#!/bin/bash

AWS_SECRETS_VAULTS=""

if [ -n "${test_encryption}" ]; then
  AWS_SECRETS_VAULTS+="drivers/csfle"
fi

if [ -n "${AWS_SECRETS_VAULTS}" ]; then
  "$DRIVERS_TOOLS"/.evergreen/auth_aws/setup_secrets.sh ${AWS_SECRETS_VAULTS}
fi
