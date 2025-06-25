#!/usr/bin/env bash
# Run spec syncing script and create PR

# SETUP
SPEC_DEST="$(realpath -s "./test")"
SRC_URL="https://github.com/mongodb/specifications.git"
# needs to be set for resunc-specs.sh
SPEC_SRC="$(realpath -s "../specifications")"
SCRIPT="$(realpath -s "./.evergreen/resync-specs.sh")"
BRANCH_NAME="spec-resync-"$(date '+%m-%d-%Y')

## List of directories to skip
#SKIP_DIRECTORIES=("asynchronous" "__pycache__")
# we have a list of specs that we manually override *if the git diff is that specific line, then don't change it
# *ask in python channel
#SKIP_FILES=()
# ask steve for how to create PR from evergreen account(?)
# for now, treat it like a command line thing and git add *, git commit, git push

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

# run python script that actually does all the resyncing
/opt/devtools/bin/python3.11 ./.evergreen/scripts/resync-all-specs.py


## List to store names of specs that were changed or errored during change
#changed_specs=()
#errored_specs=()
#
## Create branch and switch to it
##git checkout -b $BRANCH_NAME 2>/dev/null || git checkout $BRANCH_NAME
#
#for item in "$SPEC_DEST"/*; do
#  item_name=$(basename "$item")
#  if [[ " ${SKIP_DIRECTORIES[*]} " =~ ${item_name} ]]; then
#    continue
#  fi
#
#  # Check that item is not a python file
#  if [[ $item != *.py ]]; then
#    echo " doing $item_name"
#    output=$($SCRIPT "$item_name" 2>&1)
#    # Check if the script ran successfully
#    if [[ $? -ne 0 ]]; then
#      echo "an error occurred"
##      errored_specs+=("$item_name\n\`\`\`$output\`\`\`\n\n")
#      errored_specs+=("$item_name\n")
#    fi
#  fi
#done
#
## change this to python, call the python script here, then pass it forward to create-pr script.
#
## Output the list of changed specs
#if git diff --quiet && [[ ${#errored_specs[@]} -eq 0 ]]; then
#  # no changes made and no errors
#  exit 0
#fi
#
#pr_body='Spec sync results:\n\n'
#if ! git diff --quiet; then
#  pr_body+="The following specs were changed:\n"
#  pr_body+="$(git diff --name-only | awk -F'/' '{print $2}' | sort | uniq | tr '\n' '\\n')"
#fi
#
## Output the list of errored specs
#if [[ ${#errored_specs[@]} -gt 0 ]]; then
#  pr_body+="\n\nThe following spec syncs encountered errors:\n"
#  for spec in "${errored_specs[@]}"; do
#    pr_body+=" - $spec\n"
#  done
#else
#  pr_body+="\nNo errors were encountered in any specs syncs.\n"
#fi
#
## Output the PR body (optional step for verification)
#echo "PR body"
#echo "$pr_body" | tr '\n' '\\n'
#echo "$pr_body" | tr '\n' '\\n' >> spec_sync.txt
#
#echo "BEGINNING OF DIFF"
#git diff
#echo "END OF DIFF"
#
# call scrypt to create PR for us
.evergreen/scripts/create-pr.sh spec_sync.txt
#
#rm spec_sync.txt
#git add $SPEC_DEST
#git commit -m $BRANCH_NAME
#git push -u origin $BRANCH_NAME
#gh pr create --title "[Spec Resync] $(date '+%m-%d-%Y')" --body "Resyncing specs for review" --base main --head $BRANCH_NAME --draft

# just get it working, we can identify pain points later after we have something to work with