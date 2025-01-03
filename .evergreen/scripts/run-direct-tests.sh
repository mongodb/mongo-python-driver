#!/bin/bash

.evergreen/utils.sh

createvirtualenv "$PYTHON_BINARY" .venv

pip install -e ".[test]"

pytest
