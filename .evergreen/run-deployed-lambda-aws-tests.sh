#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

python --version
. ${DRIVERS_TOOLS}/.evergreen/run-deployed-lambda-aws-tests.sh
