#!/usr/bin/env bash
set -eu

# Open (or update) the automated pull request resyncing the spec tests.
# Arguments: 1 = resync summary text to use as the PR body.

summary_file="$1"
today="$(date '+%m-%d-%Y')"

# Shared github-app authentication, git branch and PR plumbing.
# shellcheck disable=SC1091
. "$(dirname "${BASH_SOURCE[0]}")/github-pr-util.sh"

acquire_github_token

echo "Creating the git checkout..."
branch="spec-resync-${today}"
prepare_bot_branch "$branch"

# The spec sync also updates the vendored public suffix list (cp_psl in
# resync-specs.sh), a tracked file outside test/.
if ! stage_and_commit "resyncing specs ${today}" ./test pymongo/public_suffix_list.dat; then
    echo "No spec changes to commit; skipping PR creation."
    cat "$summary_file"
    exit 0
fi
echo "Creating the git checkout... done."

push_branch "$branch"
create_or_update_pr "$branch" "[Spec Resync] ${today}" "$summary_file"
