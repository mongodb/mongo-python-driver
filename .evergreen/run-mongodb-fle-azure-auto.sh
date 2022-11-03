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
  echo "MONGODB_URI unexpectedly contains user credentials in FLE Azure test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

authtest () {
    echo "Running Azure Credential Acquisition Test with $PYTHON"
    $PYTHON --version
    $PYTHON -m pip install --upgrade wheel setuptools pip
    $PYTHON -m pip install https://github.com/blink1073/libmongocrypt/archive/refs/heads/PYTHON-3396.zip#subdirectory=bindings/python
    curl -O https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz
    mkdir libmongocrypt-all && tar xzf libmongocrypt-all.tar.gz -C libmongocrypt-all
    export PYMONGOCRYPT_LIB=$(pwd)/libmongocrypt-all/debian10/nocrypto/lib64/libmongocrypt.so
    $PYTHON -m pip install '.'
    TEST_FLE_AZURE_AUTO=1 $PYTHON test/test_on_demand_csfle.py
}

PYTHON="python3" authtest
