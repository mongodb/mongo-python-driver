#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")"
echo "Setting up system..."
bash .evergreen/scripts/configure-env.sh
source .evergreen/scripts/env.sh
bash .evergreen/scripts/prepare-resources.sh
bash $DRIVERS_TOOLS/.evergreen/setup.sh
bash .evergreen/scripts/install-dependencies.sh
popd
echo "Setting up system... done."
