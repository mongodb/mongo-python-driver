#!/bin/bash
set -eu

. .evergreen/scripts/setup-dev-env.sh
cat .evergreen/scripts/test-env.sh
exit 1
just "$@"
