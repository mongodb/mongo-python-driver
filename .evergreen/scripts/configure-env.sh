#!/bin/bash -ex

# Get the current unique version of this checkout
# shellcheck disable=SC2154
if [ "$is_patch" = "true" ]; then
    # shellcheck disable=SC2154
    CURRENT_VERSION="$(git describe)-patch-$version_id"
else
    CURRENT_VERSION=latest
fi

PROJECT_DIRECTORY="$(pwd)"
DRIVERS_TOOLS="$(dirname $PROJECT_DIRECTORY)/drivers-tools"

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
    DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
fi

SCRIPT_DIR="$PROJECT_DIRECTORY/.evergreen/scripts"

if [ -f "$SCRIPT_DIR/env.sh" ]; then
  echo "Reading $SCRIPT_DIR/env.sh file"
  . "$SCRIPT_DIR/env.sh"
  exit 0
fi

export MONGO_ORCHESTRATION_HOME="$DRIVERS_TOOLS/.evergreen/orchestration"
export MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

cat <<EOT > $SCRIPT_DIR/env.sh
set -o errexit
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
export CURRENT_VERSION="$CURRENT_VERSION"
export SKIP_LEGACY_SHELL=1
export DRIVERS_TOOLS="$DRIVERS_TOOLS"
export MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
export MONGODB_BINARIES="$MONGODB_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"

export TMPDIR="$MONGO_ORCHESTRATION_HOME/db"
export PATH="$MONGODB_BINARIES:$PATH"
# shellcheck disable=SC2154
export PROJECT="$project"
export PIP_QUIET=1
EOT

# Add these expansions to make it easier to call out tests scripts from the EVG yaml
cat <<EOT > expansion.yml
DRIVERS_TOOLS: "$DRIVERS_TOOLS"
PROJECT_DIRECTORY: "$PROJECT_DIRECTORY"
EOT
