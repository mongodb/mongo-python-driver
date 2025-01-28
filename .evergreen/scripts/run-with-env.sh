#!/bin/bash -eu

# Example use: bash run-with-env.sh run-tests.sh {args...}

# Parameter expansion to get just the current directory's name
if [ "${PWD##*/}" == "src" ]; then
  . .evergreen/scripts/env.sh
  if [ -f ".evergreen/scripts/test-env.sh" ]; then
    . .evergreen/scripts/test-env.sh
  fi
else
  . src/.evergreen/scripts/env.sh
  if [ -f "src/.evergreen/scripts/test-env.sh" ]; then
    . src/.evergreen/scripts/test-env.sh
  fi
fi

set -eu

# shellcheck source=/dev/null
. "$@"
