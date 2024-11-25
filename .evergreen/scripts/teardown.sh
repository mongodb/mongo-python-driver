#!/bin/bash

# Teardown AWS
pushd "${DRIVERS_TOOLS}/.evergreen/auth_aws" || true
if [ -f "./aws_e2e_setup.json" ]; then
  . ./activate-authawsvenv.sh
  python ./lib/aws_assign_instance_profile.py
fi

popd || true

# Remove all Docker images
DOCKER=$(command -v docker) || true
if [ -n "$DOCKER" ]; then
  docker rmi -f "$(docker images -a -q)" &> /dev/null || true
fi
