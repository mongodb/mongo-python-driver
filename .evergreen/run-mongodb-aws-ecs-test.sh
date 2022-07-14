#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
export MONGODB_URI="$1"

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in ECS test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

# Install python3.7 with pip.
apt-get update
apt install python3.7 python3-pip -y

authtest () {
    echo "Running MONGODB-AWS ECS authentication tests with $PYTHON"
    $PYTHON --version
    $PYTHON -m pip install --upgrade wheel setuptools pip
    cd src
    sudo apt-get install git || true
    $PYTHON -m pip install '.[aws]'
    $PYTHON test/auth_aws/test_auth_aws.py -v
    cd -
}

PYTHON="python3.7" authtest
