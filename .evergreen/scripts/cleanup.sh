#!/bin/bash
# Clean up resources at the end of an evergreen run.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

rm -rf "${DRIVERS_TOOLS}" || true
rm -f $HERE/../../secrets-export.sh || true
