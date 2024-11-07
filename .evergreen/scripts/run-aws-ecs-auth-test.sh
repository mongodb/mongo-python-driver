#!/bin/bash

# shellcheck disable=SC2154
if [ "${skip_ECS_auth_test}" = "true" ]; then
    echo "This platform does not support the ECS auth test, skipping..."
    exit 0
fi
. .evergreen/scripts/env.sh
set -ex
cd $DRIVERS_TOOLS/.evergreen/auth_aws
. ./activate-authawsvenv.sh
. aws_setup.sh ecs
export MONGODB_BINARIES="$MONGODB_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
python aws_tester.py ecs
cd -
