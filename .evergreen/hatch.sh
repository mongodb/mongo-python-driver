#!/bin/bash
set -eu

if [ ! -x "$(command -v hatch)" ]; then
  . ${DRIVERS_TOOLS}/.evergreen/venv-utils.sh
  venvactivate .venv
fi
hatch run "$@"
