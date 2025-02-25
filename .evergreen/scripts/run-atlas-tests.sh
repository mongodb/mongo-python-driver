#!/bin/bash

# Disable xtrace for security reasons (just in case it was accidentally set).
set +x
set -o errexit
bash "${DRIVERS_TOOLS}"/.evergreen/auth_aws/setup_secrets.sh drivers/atlas_connect
bash "${PROJECT_DIRECTORY}"/.evergreen/just.sh setup-tests atlas
bash "${PROJECT_DIRECTORY}"/.evergreen/just.sh run-tests
