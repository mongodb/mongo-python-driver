#!/bin/bash
# Clean up resources at the end of an evergreen run.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

# Don't delete the in-tree submodule; clean the ignored credential and state
# files (secrets-export.sh, AWS creds, token files) that `git submodule update`
# leaves behind, so they can't carry into later tasks on a reused host. A
# caller-provided DRIVERS_TOOLS wins, so the checkout actually used is cleaned.
: "${DRIVERS_TOOLS:=$HERE/../../drivers-evergreen-tools}"
rm -f $HERE/../../secrets-export.sh || true
git -C "$DRIVERS_TOOLS" clean -fdx 2>/dev/null || true
