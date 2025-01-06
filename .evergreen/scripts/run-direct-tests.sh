#!/bin/bash
set -x
. .evergreen/utils.sh

. .evergreen/scripts/env.sh
createvirtualenv "$PYTHON_BINARY" .venv

export PYMONGO_C_EXT_MUST_BUILD=1
pip install -e ".[test]"
pytest -v
