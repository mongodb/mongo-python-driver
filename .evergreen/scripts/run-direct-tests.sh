#!/bin/bash
set -x
. .evergreen/utils.sh

env
# . .evergreen/scripts/env.sh
ls pymongo
createvirtualenv "$PYTHON_BINARY" .venv
ls pymongo
# pip install -e ".[test]"
ulimit -c unlimited
python --version
# pip install pytest
export PYMONGO_C_EXT_MUST_BUILD=1
pip install setuptools
python _setup.py build_ext -i
pip install -v -e .
ls pymongo
# python -c "import pymongo"
python -c "import bson"
# pytest --version

pytest -v
