#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

. $ROOT/.evergreen/utils.sh
UV=$(get_uv)

$UV run $HERE/run_server.py "$@"
