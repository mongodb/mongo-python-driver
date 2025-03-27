#!/bin/bash
# Keep the synchronous folders in sync with there async counterparts.
set -eu

python ./tools/synchro.py "$@"
python -m ruff check pymongo/synchronous/ gridfs/synchronous/ test/ --fix --silent
python -m ruff format pymongo/synchronous/ gridfs/synchronous/ test/ --silent
