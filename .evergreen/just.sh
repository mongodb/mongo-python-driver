#!/bin/bash
set -eu

. .evergreen/scripts/setup-dev-env.sh
# TODO remove before merging.
cat .evergreen/scripts/test-env.sh
just "$@"
