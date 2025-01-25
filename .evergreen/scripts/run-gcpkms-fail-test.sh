#!/bin/bash
HERE=$(dirname ${BASH_SOURCE:-$0})
. .$HERE/env.sh
export PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3
export LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SUCCESS=false TEST_FLE_GCP_AUTO=1 bash $HERE/scripts/setup-tests.sh
bash ./.evergreen/just.sh test-eg
