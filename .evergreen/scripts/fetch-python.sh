#!/bin/bash
# Download a Python build from python-build-standalone and print the directory
# uv should use for it.  Temporary: uv does not yet index Python 3.15
# pre-releases, so we fetch the build ourselves until the version is available
# in the Evergreen toolchain.  See PYTHON-6077.
set -eu

version="${UV_PYTHON:?UV_PYTHON must be set}"
# python-build-standalone release tag that carries the builds below.
pbs_tag="20260901"

case "$version" in
  3.15.0rc2t) interpreter="3.15.0rc2"; variant="-freethreaded" ;;
  3.15.0rc2) interpreter="3.15.0rc2"; variant="" ;;
  *) echo "No python-build-standalone build configured for Python $version" >&2; exit 1 ;;
esac

case "$(uname -s)" in
  Darwin) target_os="-apple-darwin"; base_dir="${HOME}" ;;
  Linux) target_os="-unknown-linux-gnu"; base_dir="${HOME}" ;;
  MSYS* | MINGW* | CYGWIN*) target_os="-pc-windows-msvc"; base_dir="${USERPROFILE:-$HOME}" ;;
  *) echo "Unsupported platform $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  arm64 | aarch64) target_arch="aarch64" ;;
  x86_64) target_arch="x86_64" ;;
  *) echo "Unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

asset="cpython-${interpreter}+${pbs_tag}-${target_arch}${target_os}${variant}-install_only.tar.gz"
url="https://github.com/astral-sh/python-build-standalone/releases/download/${pbs_tag}/${asset//+/%2B}"

# Use a native path on Windows: bash reports MSYS paths (e.g. /home/user)
# that the native Windows uv binary cannot resolve. Normalize backslashes
# to forward slashes, which both uv and the shell's tar accept.
dest="${base_dir}/.cache/python-build-standalone/${version}/${target_arch}${target_os}"
dest="$(printf '%s' "$dest" | tr '\\' '/')"
if [ ! -d "$dest/python" ]; then
  mkdir -p "$dest"
  echo "Downloading Python ${version} from python-build-standalone" >&2
  curl -fL --retry 3 "$url" | tar -xz -C "$dest"
fi
echo "$dest/python"
