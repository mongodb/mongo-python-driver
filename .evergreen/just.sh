#!/bin/bash
set -eu

. .evergreen/scripts/setup-dev-env.sh evergreen
set -x
cat .evergreen/scripts/env.sh
set +x
just "$@"
