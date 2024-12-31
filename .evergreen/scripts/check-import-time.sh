#!/bin/bash

. .evergreen/scripts/env.sh
set -x
export BASE_SHA="$1"
export HEAD_SHA="$2"
bash .evergreen/run-import-time-test.sh
