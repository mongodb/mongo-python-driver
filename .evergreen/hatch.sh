#!/bin/bash
set -eu

. .evergreen/scripts/ensure-hatch.sh
hatch run "$@"
