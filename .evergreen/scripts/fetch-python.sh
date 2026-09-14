#!/bin/bash
# Download a Python build from python-build-standalone's latest release and
# print the interpreter directory for uv to use.  Invoked when uv cannot
# provide the requested version, for example a pre-release uv has not indexed
# yet.
set -eu

request="${UV_PYTHON:?UV_PYTHON must be set}"

# Free-threaded builds are published under a separate asset.
variant=""
version="$request"
case "$version" in
  *t)
    variant="-freethreaded"
    version="${version%t}"
    ;;
esac

case "$(uname -s)" in
  Darwin) target_os="-apple-darwin"; base_dir="${HOME}" ;;
  Linux) target_os="-unknown-linux-gnu"; base_dir="${HOME}" ;;
  MSYS* | MINGW* | CYGWIN*) target_os="-pc-windows-msvc"; base_dir="${USERPROFILE:-$HOME}" ;;
  *) echo "Unsupported platform $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  arm64 | aarch64) target_arch="aarch64" ;;
  x86_64)
    if [ "Windows_NT" = "${OS:-}" ] && [ -n "${IS_WIN32:-}" ]; then
      target_arch="i686"
    else
      target_arch="x86_64"
    fi
    ;;
  *) echo "Unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

# The latest python-build-standalone release carries one build per minor
# version, e.g. cpython-3.15.0rc2+20260901-aarch64-apple-darwin-install_only.tar.gz.
release="$(curl -fsSL --retry 3 https://api.github.com/repos/astral-sh/python-build-standalone/releases/latest)"
tag="$(printf '%s' "$release" | sed -nE 's/.*"tag_name": *"([^"]+)".*/\1/p')"
asset="$(printf '%s' "$release" \
  | grep -oE "cpython-${version//./\\.}\\.[^\"]*-${target_arch}${target_os}${variant}-install_only\\.tar\\.gz" \
  | head -1)"

if [ -z "$asset" ]; then
  echo "No python-build-standalone build for Python $request" >&2
  exit 1
fi

url="https://github.com/astral-sh/python-build-standalone/releases/download/${tag}/${asset//+/%2B}"

# Use a native path on Windows: bash reports MSYS paths (e.g. /home/user) that
# the native Windows uv binary cannot resolve. Normalize backslashes to forward
# slashes, which both uv and the shell's tar accept.
dest="${base_dir}/.cache/python-build-standalone/${request}/${target_arch}${target_os}"
dest="$(printf '%s' "$dest" | tr '\\' '/')"
if [ ! -d "$dest/python" ]; then
  mkdir -p "$dest"
  echo "Downloading Python $request from python-build-standalone" >&2
  curl -fL --retry 3 "$url" | tar -xz -C "$dest"
fi
echo "$dest/python"
