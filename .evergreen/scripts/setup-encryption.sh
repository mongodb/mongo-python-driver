#!/bin/bash

set -eu

. .evergreen/scripts/env.sh
if [ -n "${test_encryption}" ]; then
  ./.evergreen/hatch.sh encryption:setup &
fi
