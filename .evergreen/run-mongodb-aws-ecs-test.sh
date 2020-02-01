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

if command -v virtualenv ; then
    VIRTUALENV=$(command -v virtualenv)
else
    echo "Installing virtualenv..."
    apt install python3-pip -y
    pip3 install --user virtualenv
    VIRTUALENV='python3 -m virtualenv'
fi

authtest () {
    echo "Running MONGODB-AWS ECS authentication tests with $PYTHON"
    $PYTHON --version

    $VIRTUALENV -p $PYTHON --system-site-packages --never-download venvaws
    . venvaws/bin/activate
    pip install requests botocore

    cd src
    python test/auth_aws/test_auth_aws.py
    cd -
    deactivate
    rm -rf venvaws
}

PYTHON=$(command -v python) authtest
PYTHON=$(command -v python3) authtest
