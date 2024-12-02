#!/bin/bash
set -eu

. ${DRIVERS_TOOLS}/.evergreen/venv-utils.sh
venvactivate .venv
python -m hatch run "$@"
