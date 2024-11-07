#!/bin/bash

. src/.evergreen/scripts/env.sh

cd "${DRIVERS_TOOLS}/.evergreen/auth_aws" || exit
if [ -f "./aws_e2e_setup.json" ]; then
  . ./activate-authawsvenv.sh
  python ./lib/aws_assign_instance_profile.py
fi
