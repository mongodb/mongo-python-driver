#!/bin/bash
# Clean up resources at the end of an evergreen run.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

# DRIVERS_TOOLS now points inside the checkout (the drivers-evergreen-tools
# submodule); deleting it would corrupt the workdir for later tasks on the
# same host, so it is intentionally not removed here.
#
# The tools scripts write ignored credential and state files inside the tools
# checkout (secrets-export.sh — csfle's setup-secrets.sh appends Azure client
# secrets to it — plus token_file.txt and AWS creds json), and
# `git submodule update` does not remove ignored files, so remove them
# explicitly to keep credentials from carrying into later tasks on a reused
# host. Default to the submodule when unset; a caller-provided DRIVERS_TOOLS
# (including the value baked into env.sh) wins, so the checkout actually used
# is the one cleaned.
: "${DRIVERS_TOOLS:=$HERE/../../drivers-evergreen-tools}"
rm -f $HERE/../../secrets-export.sh || true
find "$DRIVERS_TOOLS" -name secrets-export.sh -delete 2>/dev/null || true
rm -f "$DRIVERS_TOOLS/.evergreen/auth_aws/creds.json" \
  "$DRIVERS_TOOLS/.evergreen/auth_aws/aws_e2e_setup.json" \
  "$DRIVERS_TOOLS/.evergreen/auth_oidc/azure/env.sh" \
  "$DRIVERS_TOOLS/.evergreen/auth_oidc/azure/keyfile" \
  "$DRIVERS_TOOLS/token_file.txt" 2>/dev/null || true
