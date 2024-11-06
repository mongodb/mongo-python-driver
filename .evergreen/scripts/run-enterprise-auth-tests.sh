#!/bin/bash

# Disable xtrace for security reasons (just in case it was accidentally set).
set +x
bash ${DRIVERS_TOOLS}/.evergreen/auth_aws/setup_secrets.sh drivers/enterprise_auth
TEST_ENTERPRISE_AUTH=1 AUTH=auth bash "${PROJECT_DIRECTORY}"/.evergreen/hatch.sh test:test-eg
