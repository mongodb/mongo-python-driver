#!/bin/bash
set -eu
HERE=$(dirname ${BASH_SOURCE:-$0})
. $HERE/env.sh
SUCCESS=false TEST_FLE_GCP_AUTO=1 bash $HERE/setup-tests.sh
bash ./.evergreen/just.sh test-eg
