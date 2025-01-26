#!/bin/bash
set -eux

echo "Hello from just.sh"
env
. .evergreen/scripts/setup-dev-env.sh
just "$@"
