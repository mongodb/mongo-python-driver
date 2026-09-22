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
DRIVERS_TOOLS_BINARIES="$DRIVERS_TOOLS/.bin"
MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME or
# have binaries shared across tasks, so use a TMPDIR. On non-CI hosts
# (spawn hosts, VMs such as GCP/Azure, and local dev), use the conventional
# ~/.local/bin which tools on the PATH (or the shell rc) can find.
if [ "${CI:-}" == "true" ]; then
  PYMONGO_BIN_DIR="${TMPDIR:-/tmp}"/pymongo_bin
else
  PYMONGO_BIN_DIR=$HOME/.local/bin
fi

# Add the latest MongoDB toolchain bin dir to PATH if it exists, so that hosts
# with an old system Python (e.g. RHEL8's 3.6) still get a modern interpreter
# for tool installs like `uv tool install rust-just`. It goes after
# PYMONGO_BIN_DIR so the pinned uv (installed there by setup-uv.py) takes
# precedence over the toolchain's uv.
. "$(dirname "${BASH_SOURCE:-$0}")/toolchain-bin.sh"
_toolchain_bin="$(mongodb_toolchain_bin)"
if [ -d "$_toolchain_bin" ]; then
  PATH_EXT="$MONGODB_BINARIES:$PYMONGO_BIN_DIR:$_toolchain_bin:$DRIVERS_TOOLS_BINARIES:\$PATH"
else
  PATH_EXT="$MONGODB_BINARIES:$PYMONGO_BIN_DIR:$DRIVERS_TOOLS_BINARIES:\$PATH"
fi

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
    CARGO_HOME=$(cygpath -m $CARGO_HOME)
    DRIVERS_TOOLS_BINARIES=$(cygpath -m "$DRIVERS_TOOLS_BINARIES")
    MONGODB_BINARIES=$(cygpath -m "$MONGODB_BINARIES")
    PYMONGO_BIN_DIR=$(cygpath -u "$PYMONGO_BIN_DIR")
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
