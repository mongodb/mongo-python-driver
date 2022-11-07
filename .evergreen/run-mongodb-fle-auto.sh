#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       MONGODB_URI             Set the URI, including an optional username/password to use to connect to the server
#       SUCCESS                 Whether the authentication is expected to succeed or fail.  One of "true" or "false"
############################################
#            Main Program                  #
############################################

if [[ -z "$MONGODB_URI" ]]; then
    echo "Must define MONGODB_URI"
    exit 1
fi

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in FLE test!";
  exit 1
fi

authtest () {
    echo "Running Credential Acquisition Test with $PYTHON"
    $PYTHON --version
    $PYTHON -m pip install --upgrade wheel setuptools pip
    $PYTHON -m pip install https://github.com/mongodb/libmongocrypt/archive/refs/heads/master.zip#subdirectory=bindings/python
    curl -O https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz
    mkdir libmongocrypt-all && tar xzf libmongocrypt-all.tar.gz -C libmongocrypt-all
    $PYTHON -m pip install '.'
    PYMONGOCRYPT_LIB=$(pwd)/libmongocrypt-all/debian10/nocrypto/lib/libmongocrypt.so $PYTHON test/test_on_demand_csfle.py
}

PYTHON="python3" authtest
