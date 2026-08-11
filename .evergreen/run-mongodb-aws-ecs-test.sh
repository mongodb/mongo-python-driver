#!/bin/bash
# Script run on an ECS host to test MONGODB-AWS.
set -eu

############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
export MONGODB_URI="$1"

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in ECS test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

# Install a C compiler (for the C extensions) and git (needed by uv to
# resolve the mockupdb git dependency)
apt-get -qq update  < /dev/null > /dev/null
apt-get -q install -y build-essential git

export SET_XTRACE_ON=1
export CI=true
cd src
rm -rf .venv
# Discard any lockfile written by the host so we resolve from scratch
rm -f uv.lock
rm -f .evergreen/scripts/test-env.sh || true
rm -f .evergreen/scripts/env.sh || true
bash ./.evergreen/just.sh setup-tests auth_aws ecs-remote
bash .evergreen/just.sh run-tests
