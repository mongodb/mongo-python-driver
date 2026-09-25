#!/bin/bash
# Stop a server that was started using run-mongodb.sh in DRIVERS_TOOLS.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

# Default to the drivers-evergreen-tools submodule when unset.
: "${DRIVERS_TOOLS:=$ROOT/drivers-evergreen-tools}"

bash ${DRIVERS_TOOLS}/.evergreen/run-mongodb.sh stop
