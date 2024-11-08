#!/bin/bash

# Example use: bash run-with-env.sh run-tests.sh {args...}

# Parameter expansion to get just the current directory's name
if [ "${PWD##*/}" == "src" ]; then
  . .evergreen/scripts/env.sh
else
  . src/.evergreen/scripts/env.sh
fi

set -eu

# shellcheck source=/dev/null
. "$1" "${@:2}"
