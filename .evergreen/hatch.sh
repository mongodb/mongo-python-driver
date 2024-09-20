#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -x

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
if ! command -v hatch > /dev/null ; then
  platform="$(uname -s)-$(uname -m)"
  case $platform in
    Linux-x86_64)
      target=x86_64-unknown-linux-gnu
      ;;
    Linux-aarch64)
      target=aarch64-unknown-linux-gnu
      ;;
    Linux-ppc64le)
      target=powerpc64le-unknown-linux-gnu
      ;;
    CYGWIN_NT*)
      target=x86_64-pc-windows-msvc
      ;;
    Darwin-x86_64)
      target=x86_64-apple-darwin
      ;;
    Darwin-arm64)
      target=aarch64-apple-darwin
      ;;
    *)
      echo "Unsupported platform: $platform"
      exit 1
      ;;
  esac
  curl -L -o hatch.tgz https://github.com/pypa/hatch/releases/download/hatch-v1.12.0/hatch-$target.tar.gz
  tar xfz  hatch.tgz
  mv hatch .bin
  rm  hatch.tgz
fi

HATCH_PYTHON="$PYTHON_BINARY" hatch run "$@"
