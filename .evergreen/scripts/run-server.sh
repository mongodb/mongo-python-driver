#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi


${DRIVERS_TOOLS}/.evergreen/mongodl --out mongodb-bin --strip-path-components 2
set -x
./mongodb-bin/mongod --version
exit 1

UV=${UV_BINARY:-uv}
$UV run $HERE/run_server.py "$@"
