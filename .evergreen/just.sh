#!/bin/bash
set -eux

. .evergreen/scripts/setup-dev-env.sh
# TODO remove before merging.
cat .evergreen/scripts/test-env.sh
just "$@"
