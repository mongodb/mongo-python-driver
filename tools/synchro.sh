#!/bin/bash

python ./tools/synchro.py
python -m ruff check pymongo/synchronous/ gridfs/synchronous/ test/synchronous --fix --silent
python -m ruff format pymongo/synchronous/ gridfs/synchronous/ test/synchronous --silent
