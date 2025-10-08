#!/bin/bash
set -eu

bash .evergreen/scripts/setup-uv-python.sh
. .evergreen/scripts/setup-dev-env.sh

just "$@"
