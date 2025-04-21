#!/bin/bash
set -eu

. .evergreen/scripts/setup-dev-env.sh
${JUST_BINARY:-just} "$@"
