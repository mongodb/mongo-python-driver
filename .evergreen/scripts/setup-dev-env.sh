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

# Initialize the submodule (Evergreen's git.get_project does not); tolerate
# non-git hosts with a warning.
if ! git -C "$ROOT" submodule update --init --recursive; then
  echo "WARNING: could not initialize the drivers-evergreen-tools submodule;" \
    "set DRIVERS_TOOLS to a drivers-evergreen-tools checkout instead."
fi

# Mirror configure-env.sh's uv config boundary; see it for the full explanation.
if [ -d "$ROOT/drivers-evergreen-tools" ] && [ ! -f "$ROOT/drivers-evergreen-tools/uv.toml" ]; then
  cat <<EOT > "$ROOT/drivers-evergreen-tools/uv.toml"
# Written by mongo-python-driver to stop uv's config discovery here; see
# .evergreen/scripts/configure-env.sh.
EOT
fi

# Keep the boundary out of git status via the submodule's local exclude;
# no-op without git.
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
