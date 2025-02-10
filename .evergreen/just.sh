#!/bin/bash
set -eu

bash .evergreen/scripts/setup-dev-env.sh
just "$@"
