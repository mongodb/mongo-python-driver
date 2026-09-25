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
# Default to the submodule checkout; an env var override wins, mirroring
# install-dependencies.sh, run-getdata.sh, stop-server.sh, and utils.py.
DRIVERS_TOOLS="${DRIVERS_TOOLS:-$PROJECT_DIRECTORY/drivers-evergreen-tools}"
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

# Cygwin spelling for bash PATH entries; PYMONGO_BIN_DIR itself is converted
# to native form below for consumers like uv.
if [ "Windows_NT" = "${OS:-}" ]; then
  PYMONGO_BIN_DIR_POSIX="$(cygpath -u "$PYMONGO_BIN_DIR")"
else
  PYMONGO_BIN_DIR_POSIX="$PYMONGO_BIN_DIR"
fi

# Add the latest MongoDB toolchain bin dir to PATH if it exists, so that hosts
# with an old system Python (e.g. RHEL8's 3.6) still get a modern interpreter
# for tool installs like `uv tool install rust-just`. It goes after
# PYMONGO_BIN_DIR so the pinned uv (installed there by setup-uv.py) takes
# precedence over the toolchain's uv.
if [ "Windows_NT" = "${OS:-}" ]; then
  _toolchain_bin="/cygdrive/c/Python/Current/Scripts"
elif [ "$(uname -s)" == "Darwin" ]; then
  _toolchain_bin="/Library/Frameworks/Python.Framework/Versions/Current/bin"
else
  _toolchain_bin="/opt/python/Current/bin"
fi
if [ -d "$_toolchain_bin" ]; then
  PATH_EXT="$MONGODB_BINARIES:$PYMONGO_BIN_DIR_POSIX:$_toolchain_bin:$DRIVERS_TOOLS_BINARIES:\$PATH"
else
  PATH_EXT="$MONGODB_BINARIES:$PYMONGO_BIN_DIR_POSIX:$DRIVERS_TOOLS_BINARIES:\$PATH"
fi

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
    CARGO_HOME=$(cygpath -m $CARGO_HOME)
    DRIVERS_TOOLS_BINARIES=$(cygpath -m "$DRIVERS_TOOLS_BINARIES")
    MONGODB_BINARIES=$(cygpath -m "$MONGODB_BINARIES")
    # Native form, uniform with the paths above, for consumers like uv.
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
export PYMONGO_BIN_DIR="$PYMONGO_BIN_DIR"
export PYMONGO_BIN_DIR_POSIX="$PYMONGO_BIN_DIR_POSIX"
export PATH="$PATH_EXT"
# shellcheck disable=SC2154
export PROJECT="${project:-mongo-python-driver}"
export PIP_QUIET=1
EOT

# Initialize the drivers-evergreen-tools submodule (Evergreen's
# git.get_project does not init submodules). Checks out the gitlink recorded
# in this checkout. Tolerate non-git contexts (rsync'd spawn hosts) with a
# warning; the checkout contents are still present there.
if ! git -C "$PROJECT_DIRECTORY" submodule update --init --recursive; then
  echo "WARNING: could not initialize the drivers-evergreen-tools submodule;" \
    "using the existing checkout contents instead."
fi

# Write a uv configuration boundary into the drivers-evergreen-tools checkout.
#
# The submodule is vendored INSIDE the project directory, so uv's config
# discovery from the tools' own scripts (uv venv and uv export in
# install-cli.sh, invoked via setup.sh and run-mongodb.sh) would otherwise
# walk up out of the submodule into pyproject.toml and enforce this project's
# [tool.uv] required-version pin against whatever uv those scripts happen to
# run (the host image's uv, or the "uv~=0.8.0" shim install-cli.sh installs),
# failing on any mismatch. A uv.toml here stops that discovery at the
# submodule boundary, leaving the pin to apply only to this project's own uv
# invocations.
#
# The file is untracked in the submodule and only written when absent, so a
# submodule checkout that gains its own uv.toml makes "git submodule update"
# fail loudly rather than being silently clobbered.
if [ -d "${DRIVERS_TOOLS}" ] && [ ! -f "${DRIVERS_TOOLS}/uv.toml" ]; then
  cat <<EOT > "${DRIVERS_TOOLS}/uv.toml"
# Configuration boundary written by the mongo-python-driver scripts; see
# .evergreen/scripts/configure-env.sh. Keeps uv's config discovery from
# reaching the vendoring project's pyproject.toml and its required-version pin.
EOT
fi

# Keep the boundary out of git status: an untracked uv.toml marks the parent
# checkout dirty with modified submodule content after every setup. Add it to
# the submodule's local exclude (.git/modules/.../info/exclude) rather than
# its tracked .gitignore, so the pinned checkout stays untouched. No-op
# without git (uninitialized submodule, rsync'd spawn hosts), where there is
# no status to keep clean.
if _git_dir=$(git -C "${DRIVERS_TOOLS}" rev-parse --absolute-git-dir 2>/dev/null); then
  mkdir -p "${_git_dir}/info"
  grep -qxF "uv.toml" "${_git_dir}/info/exclude" 2>/dev/null ||
    printf "uv.toml\n" >> "${_git_dir}/info/exclude"
fi

# Write the .env file for drivers-tools.
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
