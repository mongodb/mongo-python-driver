#!/bin/bash
set -x
. .evergreen/utils.sh

# . .evergreen/scripts/env.sh
ls pymongo
createvirtualenv "$PYTHON_BINARY" .venv
ls pymongo
# pip install -e ".[test]"

python --version
# pip install pytest
pip install -v -e .
ls pymongo
python -c "import pymongo"
python -c "import bson"
# pytest --version

pytest -v
