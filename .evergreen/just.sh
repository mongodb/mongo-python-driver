#!/bin/bash
set -eux

. .evergreen/scripts/setup-dev-env.sh
just "$@"
