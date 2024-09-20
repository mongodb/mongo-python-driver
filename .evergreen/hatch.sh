#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

source .evergreen/scripts/env.sh

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

# Check if we should skip hatch and run the tests directly.
if [ -n "$SKIP_HATCH" ]; then
    ENV_NAME=testenv-$RANDOM
    createvirtualenv "$PYTHON_BINARY" $ENV_NAME
    # shellcheck disable=SC2064
    trap "deactivate; rm -rf $ENV_NAME" EXIT HUP
    python -m pip install -e ".[test]"
    bash ./.evergreen/run-tests.sh
    exit 0
fi

# Bootstrap hatch if needed.
if [ ! -f .bin/hatch ] && [ ! -f .bin/hatch.exe ] ; then
  platform="$(uname -s)-$(uname -m)"
  case $platform in
    Linux-x86_64)
      target=x86_64-unknown-linux-gnu.tar.gz
      ;;
    Linux-aarch64)
      target=aarch64-unknown-linux-gnu.tar.gz
      ;;
    CYGWIN_NT*)
      target=x86_64-pc-windows-msvc.zip
      ;;
    Darwin-x86_64)
      target=x86_64-apple-darwin.tar.gz
      ;;
    Darwin-arm64)
      target=aarch64-apple-darwin.tar.gz
      ;;
    *)
      echo "Unsupported platform: $platform"
      exit 1
      ;;
  esac
  curl -L -o hatch.bin https://github.com/pypa/hatch/releases/download/hatch-v1.12.0/hatch-$target
  mkdir -p .bin
  if [ "${OS:-}" == "Windows_NT" ]; then
    unzip hatch.bin
    mv hatch.exe .bin
    .bin/hatch.exe --version
  else
    tar xfz hatch.bin
    mv hatch .bin
    .bin/hatch --version
  fi
  rm hatch.bin
fi

if [ "${OS:-}" == "Windows_NT" ]; then
  HATCH=".bin/hatch.exe"
else
  HATCH="./bin/hatch"
fi
HATCH_PYTHON="$PYTHON_BINARY" $HATCH run "$@"
