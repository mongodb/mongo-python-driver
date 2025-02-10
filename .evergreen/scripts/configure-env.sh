#!/bin/bash

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

echo "HI: $PYMONGO_BIN_DIR, $CI"
exit 1
# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
    CARGO_HOME=$(cygpath -m $CARGO_HOME)
    UV_TOOL_DIR=$(cygpath -m "$UV_TOOL_DIR")
    UV_CACHE_DIR=$(cygpath -m "$UV_CACHE_DIR")
    PYMONGO_BIN_DIR=$(cygpath -m $PYMONGO_BIN_DIR)
fi

SCRIPT_DIR="$PROJECT_DIRECTORY/.evergreen/scripts"

if [ -f "$SCRIPT_DIR/env.sh" ]; then
  echo "Reading $SCRIPT_DIR/env.sh file"
  . "$SCRIPT_DIR/env.sh"
  exit 0
fi

MONGO_ORCHESTRATION_HOME="$DRIVERS_TOOLS/.evergreen/orchestration"
DRIVERS_TOOLS_BINARIES="$DRIVERS_TOOLS/.bin"
MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  PYMONGO_BIN_DIR=${DRIVERS_TOOLS_BINARIES:-}
# We want to use a path that's already on PATH on spawn hosts.
else
  PYMONGO_BIN_DIR=$HOME/cli_bin
fi


PATH="$MONGODB_BINARIES:$DRIVERS_TOOLS_BINARIES:$PYMONGO_BIN_DIR:$PATH"

cat <<EOT > "$SCRIPT_DIR"/env.sh
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
export CURRENT_VERSION="$CURRENT_VERSION"
export SKIP_LEGACY_SHELL=1
export DRIVERS_TOOLS="$DRIVERS_TOOLS"
export MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
export MONGODB_BINARIES="$MONGODB_BINARIES"
export DRIVERS_TOOLS_BINARIES="$DRIVERS_TOOLS_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
export SETDEFAULTENCODING="${SETDEFAULTENCODING:-}"
export SKIP_CSOT_TESTS="${SKIP_CSOT_TESTS:-}"
export MONGODB_STARTED="${MONGODB_STARTED:-}"
export DISABLE_TEST_COMMANDS="${DISABLE_TEST_COMMANDS:-}"
export GREEN_FRAMEWORK="${GREEN_FRAMEWORK:-}"
export NO_EXT="${NO_EXT:-}"
export COVERAGE="${COVERAGE:-}"
export COMPRESSORS="${COMPRESSORS:-}"
export MONGODB_API_VERSION="${MONGODB_API_VERSION:-}"
export skip_crypt_shared="${skip_crypt_shared:-}"
export STORAGE_ENGINE="${STORAGE_ENGINE:-}"
export REQUIRE_API_VERSION="${REQUIRE_API_VERSION:-}"
export skip_web_identity_auth_test="${skip_web_identity_auth_test:-}"
export skip_ECS_auth_test="${skip_ECS_auth_test:-}"

export CARGO_HOME="$CARGO_HOME"
export TMPDIR="$MONGO_ORCHESTRATION_HOME/db"
export PYMONGO_BIN_DIR="$PYMONGO_BIN_DIR"
export UV_TOOL_DIR="$UV_TOOL_DIR"
export UV_CACHE_DIR="$UV_CACHE_DIR"
export UV_TOOL_BIN_DIR="$DRIVERS_TOOLS_BINARIES"
export PATH="$PATH"
# shellcheck disable=SC2154
export PROJECT="${project:-mongo-python-driver}"
export PIP_QUIET=1
EOT

# Skip CSOT tests on non-linux platforms.
if [ "$(uname -s)" != "Linux" ]; then
    echo "export SKIP_CSOT_TESTS=1" >> $SCRIPT_DIR/env.sh
fi

# Add these expansions to make it easier to call out tests scripts from the EVG yaml
cat <<EOT > expansion.yml
DRIVERS_TOOLS: "$DRIVERS_TOOLS"
PROJECT_DIRECTORY: "$PROJECT_DIRECTORY"
EOT
