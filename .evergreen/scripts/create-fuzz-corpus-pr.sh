#!/usr/bin/env bash
set -eu

# Open (or update) the automated pull request that grows fuzz/corpus/
# with freshly harvested units. Arguments: 1 = harvested units directory,
# 2 = harvest summary markdown to use as the PR body.

harvest_dir="$1"
summary_file="$2"
today="$(date '+%m-%d-%Y')"

# Shared github-app authentication, git branch and PR plumbing.
# shellcheck disable=SC1091
. "$(dirname "${BASH_SOURCE[0]}")/github-pr-util.sh"

acquire_github_token

echo "Creating the git checkout..."
branch="fuzz-corpus-${today}"
prepare_bot_branch "$branch"

mkdir -p fuzz/corpus
cp "$harvest_dir"/* fuzz/corpus/
if ! stage_and_commit "PYTHON-6178 Grow the BSON fuzz corpus (${today})" fuzz/corpus; then
    echo "No new corpus units to commit; skipping PR creation."
    exit 0
fi
echo "Creating the git checkout... done."

push_branch "$branch"
create_or_update_pr "$branch" "[Fuzz Corpus] ${today}" "$summary_file"
