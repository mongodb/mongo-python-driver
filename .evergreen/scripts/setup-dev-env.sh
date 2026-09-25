#!/bin/bash
# Set up development environment.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Get variables defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# The bin dir for the pinned uv/just. setup-system.sh sets it on evergreen hosts;
# default it here so local dev (without setup-system.sh) also has a usable value.
# Native (Windows) form on cygwin, like install-dependencies.sh.
export PYMONGO_BIN_DIR="${PYMONGO_BIN_DIR:-$HOME/.local/bin}"
if [ "Windows_NT" = "${OS:-}" ]; then
  _bin_dir="$(cygpath -m "$PYMONGO_BIN_DIR")"
  export PYMONGO_BIN_DIR="$_bin_dir"
  _posix_bin_dir="$(cygpath -u "$_bin_dir")"
  export PYMONGO_BIN_DIR_POSIX="$_posix_bin_dir"
else
  export PYMONGO_BIN_DIR_POSIX="$PYMONGO_BIN_DIR"
fi

# install-dependencies.sh runs as a child process, so its PATH changes do not
# propagate back here: ensure the bin dir is on this process's PATH too, so a
# fresh install (first run, bin dir not on PATH yet) is visible to the
# `uv sync` and pre-commit setup below.
case ":$PATH:" in
  *":$PYMONGO_BIN_DIR_POSIX:"*) ;;
  *) export PATH="$PYMONGO_BIN_DIR_POSIX:$PATH" ;;
esac

# Make sure a login shell can find the bin dir by adding it to the rc file, so
# local dev (which may never run setup-system.sh) still has it on PATH. env.sh's
# PATH does not persist past this session. Select the rc file from $SHELL (not
# by which rc file happens to exist) so the user's actual shell is updated, and
# create it if it does not exist yet.
if [ "${CI:-}" != "true" ] && [ "${GITHUB_ACTIONS:-}" != "true" ]; then
  case "${SHELL:-}" in
    */zsh) _rc="$HOME/.zshrc" ;;
    *) _rc="$HOME/.bashrc" ;;
  esac
  touch "$_rc"
  grep -qF 'export PATH="'"$PYMONGO_BIN_DIR_POSIX"':$PATH"' "$_rc" 2>/dev/null || \
    printf 'export PATH="%s:$PATH"\n' "$PYMONGO_BIN_DIR_POSIX" >> "$_rc"
fi

# Initialize the drivers-evergreen-tools submodule before
# install-dependencies.sh (which sources ensure-uv.sh from the tools
# checkout). Evergreen's git.get_project does not init submodules, so this
# must happen in our scripts. Tolerate non-git contexts (containers) with a
# warning rather than a hard failure.
if ! git -C "$ROOT" submodule update --init --recursive; then
  echo "WARNING: could not initialize the drivers-evergreen-tools submodule;" \
    "set DRIVERS_TOOLS to a drivers-evergreen-tools checkout instead."
fi

# Mirror configure-env.sh: write the uv configuration boundary into the
# submodule checkout so uv's config discovery from the tools' own scripts
# (which run uv versions we do not pin) cannot reach the project's
# [tool.uv] required-version pin. Only meaningful when the submodule checkout
# exists; see configure-env.sh for the full explanation.
if [ -d "$ROOT/drivers-evergreen-tools" ] && [ ! -f "$ROOT/drivers-evergreen-tools/uv.toml" ]; then
  cat <<EOT > "$ROOT/drivers-evergreen-tools/uv.toml"
# Configuration boundary written by the mongo-python-driver scripts; see
# .evergreen/scripts/configure-env.sh. Keeps uv's config discovery from
# reaching the vendoring project's pyproject.toml and its required-version pin.
EOT
fi

# Keep the boundary out of git status, as in configure-env.sh: add it to the
# submodule's local exclude so the untracked uv.toml does not mark the parent
# checkout dirty. No-op without git (uninitialized submodule, rsync'd spawn
# hosts).
if _git_dir=$(git -C "$ROOT/drivers-evergreen-tools" rev-parse --absolute-git-dir 2>/dev/null); then
  mkdir -p "${_git_dir}/info"
  grep -qxF "uv.toml" "${_git_dir}/info/exclude" 2>/dev/null ||
    printf "uv.toml\n" >> "${_git_dir}/info/exclude"
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Re-source env.sh in case a dependency install updated it, e.g. on a host
# without a toolchain where uv was installed into a shared bin dir.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Handle the value for UV_PYTHON.
. $HERE/setup-uv-python.sh

# Only run the next part if not running on CI.
if [ -z "${CI:-}" ]; then
  # Set up venv, making sure c extensions build unless disabled.
  if [ -z "${NO_EXT:-}" ]; then
    export PYMONGO_C_EXT_MUST_BUILD=1
  fi

  (
    cd $ROOT && uv sync
  )

  # Set up build utilities on Windows spawn hosts.
  if [ -f $HOME/.visualStudioEnv.sh ]; then
    set +u
    SSH_TTY=1 source $HOME/.visualStudioEnv.sh
    set -u
  fi

  # Only set up pre-commit if we are in a git checkout.
  if [ -f $HERE/.git ]; then
    if ! command -v pre-commit &>/dev/null; then
      uv tool install pre-commit
    fi

    if [ ! -f .git/hooks/pre-commit ]; then
      uvx pre-commit install
    fi
  fi
fi
