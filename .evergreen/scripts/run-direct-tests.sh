#!/bin/bash

. .evergreen/utils.sh

. .evergreen/scripts/env.sh

createvirtualenv "$PYTHON_BINARY" .venv

# pip install -e ".[test]"

python --version
pip install pytest
export PYTHONFAULTHANDLER=1
pytest --version

pytest -v
