#!/bin/bash

# shellcheck disable=SC2154
if [ "${skip_ECS_auth_test}" = "true" ]; then
    echo "This platform does not support the ECS auth test, skipping..."
    exit 0
fi
set -ex
cd "$DRIVERS_TOOLS"/.evergreen/auth_aws
. ./activate-authawsvenv.sh
. aws_setup.sh ecs
cd -
