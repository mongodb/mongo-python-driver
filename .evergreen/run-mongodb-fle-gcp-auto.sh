#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including an optional username/password to use to connect to the server
#       SUCCESS                 Whether the authentication is expected to succeed or fail.  One of "true" or "false"
############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
export MONGODB_URI="$1"

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in FLE GCP test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

authtest () {
    echo "Running GCP Credential Acquisition Test with $PYTHON"
    $PYTHON --version
    $PYTHON -m pip install --upgrade wheel setuptools pip
    $PYTHON -m pip install '.[encryption]'
    $PYTHON -m pip install https://github.com/blink1073/libmongocrypt/archive/refs/heads/PYTHON-3367.zip#subdirectory=bindings/python
    TEST_FLE_GCP_AUTO=1 $PYTHON test/test_on_demand_csfle.py
}

PYTHON="python3" authtest
