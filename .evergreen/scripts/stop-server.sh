#!/bin/bash
# Stop a server that was started using run-orchestration.sh in DRIVERS_TOOLS.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

bash ${DRIVERS_TOOLS}/.evergreen/stop-orchestration.sh
