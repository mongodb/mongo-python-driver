#!/bin/bash

tox -e aws-secrets -- --profile "$1" drivers/csfle

source ./secrets-export.sh

export AWS_ACCESS_KEY_ID=$FLE_AWS_KEY
export AWS_SECRET_ACCESS_KEY=$FLE_AWS_SECRET
export AWS_DEFAULT_REGION=us-east-1
export AWS_SESSION_TOKEN=

tox -e encryption-tests

rm -rf libmongocrypt*
kill -9 $(pgrep -f 'python')
