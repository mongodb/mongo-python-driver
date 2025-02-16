#!/bin/bash
set -eu
HERE=$(dirname ${BASH_SOURCE:-$0})
. $HERE/env.sh
export PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3
export LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SUCCESS=false bash $HERE/setup-tests.sh kms gcp
bash ./.evergreen/just.sh test-eg
