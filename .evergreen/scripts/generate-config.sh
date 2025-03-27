#!/bin/bash
# Entry point for the generate-config pre-commit hook.

set -eu

python .evergreen/scripts/generate_config.py
