#!/bin/bash
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd $HERE
. env.sh

rm -rf $DRIVERS_TOOLS
if [ "$PROJECT" = "drivers-tools" ]; then
    # If this was a patch build, doing a fresh clone would not actually test the patch
    cp -R $PROJECT_DIRECTORY/ $DRIVERS_TOOLS
else
    git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git $DRIVERS_TOOLS
fi
echo "{ \"releases\": { \"default\": \"$MONGODB_BINARIES\" }}" >$MONGO_ORCHESTRATION_HOME/orchestration.config

popd
