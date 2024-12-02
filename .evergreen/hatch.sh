#!/bin/bash
set -eu

. .evergreen/ensure-hatch.sh
hatch run "$@"
