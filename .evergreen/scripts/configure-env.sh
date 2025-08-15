#!/bin/bash
# Configure an evergreen test environment.
set -eu

# Get the current unique version of this checkout
# shellcheck disable=SC2154
if [ "${is_patch:-}" = "true" ]; then
    # shellcheck disable=SC2154
    CURRENT_VERSION="$(git describe)-patch-$version_id"
else
    CURRENT_VERSION=latest
fi

PROJECT_DIRECTORY="$(pwd)"
DRIVERS_TOOLS="$(dirname $PROJECT_DIRECTORY)/drivers-tools"
CARGO_HOME=${CARGO_HOME:-${DRIVERS_TOOLS}/.cargo}
UV_TOOL_DIR=$PROJECT_DIRECTORY/.local/uv/tools
UV_CACHE_DIR=$PROJECT_DIRECTORY/.local/uv/cache
DRIVERS_TOOLS_BINARIES="$DRIVERS_TOOLS/.bin"
MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  PYMONGO_BIN_DIR=${DRIVERS_TOOLS_BINARIES:-}
# We want to use a path that's already on PATH on spawn hosts.
else
  PYMONGO_BIN_DIR=$HOME/cli_bin
fi

PATH_EXT="$MONGODB_BINARIES:$DRIVERS_TOOLS_BINARIES:$PYMONGO_BIN_DIR:\$PATH"

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
    CARGO_HOME=$(cygpath -m $CARGO_HOME)
    UV_TOOL_DIR=$(cygpath -m "$UV_TOOL_DIR")
    UV_CACHE_DIR=$(cygpath -m "$UV_CACHE_DIR")
    DRIVERS_TOOLS_BINARIES=$(cygpath -m "$DRIVERS_TOOLS_BINARIES")
    MONGODB_BINARIES=$(cygpath -m "$MONGODB_BINARIES")
    PYMONGO_BIN_DIR=$(cygpath -m "$PYMONGO_BIN_DIR")
fi

SCRIPT_DIR="$PROJECT_DIRECTORY/.evergreen/scripts"

if [ -f "$SCRIPT_DIR/env.sh" ]; then
  echo "Reading $SCRIPT_DIR/env.sh file"
  . "$SCRIPT_DIR/env.sh"
  exit 0
fi

export MONGO_ORCHESTRATION_HOME="$DRIVERS_TOOLS/.evergreen/orchestration"
export MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

cat <<EOT > "$SCRIPT_DIR"/env.sh
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
export CURRENT_VERSION="$CURRENT_VERSION"
export DRIVERS_TOOLS="$DRIVERS_TOOLS"
export MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
export MONGODB_BINARIES="$MONGODB_BINARIES"
export DRIVERS_TOOLS_BINARIES="$DRIVERS_TOOLS_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"

export CARGO_HOME="$CARGO_HOME"
export UV_TOOL_DIR="$UV_TOOL_DIR"
export UV_CACHE_DIR="$UV_CACHE_DIR"
export UV_TOOL_BIN_DIR="$DRIVERS_TOOLS_BINARIES"
export PYMONGO_BIN_DIR="$PYMONGO_BIN_DIR"
export PATH="$PATH_EXT"
# shellcheck disable=SC2154
export PROJECT="${project:-mongo-python-driver}"
export PIP_QUIET=1
EOT

# Write the .env file for drivers-tools.
rm -rf $DRIVERS_TOOLS
BRANCH=master
ORG=mongodb-labs
git clone --branch $BRANCH https://github.com/$ORG/drivers-evergreen-tools.git $DRIVERS_TOOLS

cat <<EOT > ${DRIVERS_TOOLS}/.env
SKIP_LEGACY_SHELL=1
DRIVERS_TOOLS="$DRIVERS_TOOLS"
MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
MONGODB_BINARIES="$MONGODB_BINARIES"
EOT

# Add these expansions to make it easier to call out tests scripts from the EVG yaml
cat <<EOT > expansion.yml
DRIVERS_TOOLS: "$DRIVERS_TOOLS"
PROJECT_DIRECTORY: "$PROJECT_DIRECTORY"
EOT

# If the toolchain is available, symlink binaries to the bin dir.  This has to be done
# after drivers-tools is cloned, since we might be using its binary dir.
_bin_path=""
if [ "Windows_NT" == "${OS:-}" ]; then
  _bin_path="/cygdrive/c/Python/Current/Scripts"
elif [ "$(uname -s)" == "Darwin" ]; then
  _bin_path="/Library/Frameworks/Python.Framework/Versions/Current/bin"
else
  _bin_path="/opt/python/Current/bin"
fi
if [ -d "${_bin_path}" ]; then
  _suffix=""
  if [ "Windows_NT" == "${OS:-}" ]; then
    _suffix=".exe"
  fi
  echo "Symlinking binaries from toolchain"
  mkdir -p $PYMONGO_BIN_DIR
  ln -s ${_bin_path}/just${_suffix} $PYMONGO_BIN_DIR/just${_suffix}
  ln -s ${_bin_path}/uv${_suffix} $PYMONGO_BIN_DIR/uv${_suffix}
  ln -s ${_bin_path}/uvx${_suffix} $PYMONGO_BIN_DIR/uvx${_suffix}
fi
