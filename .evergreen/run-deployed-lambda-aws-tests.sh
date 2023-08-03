#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

export PATH="/opt/python/3.9/bin:${PATH}"
python --version
pushd ./test/lambda

. build.sh
popd
. ${DRIVERS_TOOLS}/.evergreen/aws_lambda/run-deployed-lambda-aws-tests.sh
