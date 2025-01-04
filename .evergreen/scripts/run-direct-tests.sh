#!/bin/bash

. .evergreen/utils.sh

. .evergreen/scripts/env.sh

createvirtualenv "$PYTHON_BINARY" .venv

# pip install -e ".[test]"

python --version
pip install pytest
pip install -e .
pytest --version

pytest -v
