#!/usr/bin/env bash
# Run spec syncing script and create PR

# SETUP
SPEC_DEST="$(realpath -s "./test")"
SRC_URL="https://github.com/mongodb/specifications.git"
# needs to be set for resunc-specs.sh
SPEC_SRC="$(realpath -s "../specifications")"
SCRIPT="$(realpath -s "./.evergreen/resync-specs.sh")"
BRANCH_NAME="spec-resync-"$(date '+%m-%d-%Y')

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
/opt/devtools/bin/python3.11 ./.evergreen/scripts/resync-all-specs.py "$PR_DESC"


if [[ -f $PR_DESC ]]; then
  # changes were made -> call scrypt to create PR for us
  .evergreen/scripts/create-pr.sh "$PR_DESC"
  rm "$PR_DESC"
fi
