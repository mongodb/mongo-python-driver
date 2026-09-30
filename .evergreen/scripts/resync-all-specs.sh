#!/usr/bin/env bash
# Run spec syncing script and create PR
set -eu

# SETUP
SRC_URL="https://github.com/mongodb/specifications.git"
# needs to be set for resync-specs.sh
SPEC_SRC="$(realpath "../specifications")"
SCRIPT="$(realpath "./.evergreen/resync-specs.sh")"

# Clone the spec repo if the directory does not exist
if [[ ! -d $SPEC_SRC ]]; then
  git clone $SRC_URL $SPEC_SRC
  if [[ $? -ne 0 ]]; then
    echo "Error: Failed to clone repository."
    exit 1
  fi
fi

# Set environment variable to the cloned spec repo for resync-specs.sh
export MDB_SPECS="$SPEC_SRC"

# Check that resync-specs.sh exists and is executable
if [[ ! -x $SCRIPT ]]; then
  echo "Error: $SCRIPT not found or is not executable."
  exit 1
fi

PR_DESC="spec_sync.txt"

# run python script that actually does all the resyncing
if ! [ -n "${CI:-}" ]
then
  # we're running locally
  python3 ./.evergreen/scripts/resync-all-specs.py
else
  # Use the newest mongodbtoolchain python; the default python3 may be too old.
  PY=$(ls -1d /opt/mongodbtoolchain/v*/bin/python3 2>/dev/null | sort -V | tail -1)
  [ -n "$PY" ] || PY=python3
  [ "$("$PY" -c 'import sys; print(sys.version_info >= (3, 7))' 2>/dev/null)" = True ] \
      || { echo "Error: $PY is not Python 3.7+, which resync-all-specs.py requires." >&2; exit 1; }
  "$PY" ./.evergreen/scripts/resync-all-specs.py --filename "$PR_DESC"
  if [[ -f $PR_DESC ]]; then
    # changes were made -> call scrypt to create PR for us
    .evergreen/scripts/create-spec-pr.sh "$PR_DESC"
    rm "$PR_DESC"
  fi
fi
