#!/bin/bash

. .evergreen/utils.sh

. .evergreen/scripts/env.sh

createvirtualenv "$PYTHON_BINARY" .venv

# pip install -e ".[test]"
set -x
python --version
# pip install pytest
pip install -e .
python -c "import pymongo"
python -c "import bson"
# pytest --version

pytest -v
