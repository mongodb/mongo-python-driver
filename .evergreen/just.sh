#!/bin/bash
set -eux

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"

# Run the setup scripts.
bash $HERE/scripts/setup-uv-python.sh
bash $HERE/scripts/setup-dev-env.sh

# Source the env files to pick up common variables.
if [ -f $HERE/scripts.env.sh ]; then
  . $HERE/scripts/env.sh
fi

# Get variables defined in test-env.sh.
if [ -f $HERE/scripts.test-env.sh ]; then
  . $HERE/scriptstest-env.sh
fi

just "$@"
