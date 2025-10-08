#!/bin/bash
set -eu

. .evergreen/scripts/setup-uv-python.sh
. .evergreen/scripts/setup-dev-env.sh

just "$@"
