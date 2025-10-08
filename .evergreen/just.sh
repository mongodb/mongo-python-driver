#!/bin/bash
set -eux

bash .evergreen/scripts/setup-uv-python.sh
. .evergreen/scripts/setup-dev-env.sh

just "$@"
