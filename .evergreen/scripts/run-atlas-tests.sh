#!/bin/bash

. .evergreen/scripts/env.sh
# Disable xtrace for security reasons (just in case it was accidentally set).
set +x
set -o errexit
bash "${DRIVERS_TOOLS}"/.evergreen/auth_aws/setup_secrets.sh drivers/atlas_connect
TEST_ATLAS=1 bash "${PROJECT_DIRECTORY}"/.evergreen/hatch.sh test:test-eg
