#!/bin/bash

. .evergreen/scripts/env.sh
export PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3
export LIBMONGOCRYPT_URL=https://s3.amazonaws.com/mciuploads/libmongocrypt/debian11/master/latest/libmongocrypt.tar.gz
SKIP_SERVERS=1 bash ./.evergreen/setup-encryption.sh
SUCCESS=false TEST_FLE_GCP_AUTO=1 ./.evergreen/just.sh test-eg
