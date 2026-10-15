#!/bin/bash
# Entry point for the generate-config pre-commit hook.

set -eu

# Assert the generation rules before regenerating the config from them.
python -m pytest .evergreen/scripts/test_generate_config.py -m ""

python .evergreen/scripts/generate_config.py
