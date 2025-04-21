#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

UV=${UV_BINARY:-uv}
env
exit 1
$UV run $HERE/run_server.py "$@"
