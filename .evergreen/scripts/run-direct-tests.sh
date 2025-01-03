#!/bin/bash

.evergreen/utils.sh

. .evergreen/scripts/env.sh

createvirtualenv "$PYTHON_BINARY" .venv

pip install -e ".[test]"

pytest
