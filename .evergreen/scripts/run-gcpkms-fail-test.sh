#!/bin/bash
set -eu
HERE=$(dirname ${BASH_SOURCE:-$0})
. $HERE/env.sh
./.evergreen/just.sh setup-test kms gcp-fail
bash ./.evergreen/just.sh test-eg
