#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

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

# Install python3.10 with pip.
apt-get update
apt-get install python3.10 python3-pip build-essential python3.10-dev -y

export PYTHON_BINARY="python3.10"
export TEST_AUTH_AWS=1
export AUTH="auth"
export SET_XTRACE_ON=1
cd src
python3.10 -m pip install -q --user tox
bash .evergreen/tox.sh -m test-eg
