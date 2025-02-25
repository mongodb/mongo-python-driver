#!/bin/bash
set -eu

# Disable xtrace for security reasons (just in case it was accidentally set).
set +x
# Use the default python to bootstrap secrets.
bash "${DRIVERS_TOOLS}"/.evergreen/secrets_handling/setup-secrets.sh drivers/enterprise_auth
bash "${PROJECT_DIRECTORY}"/.evergreen/just.sh setup-tests enterprise_auth
bash "${PROJECT_DIRECTORY}"/.evergreen/just.sh run-tests
