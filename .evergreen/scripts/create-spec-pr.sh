#!/usr/bin/env bash
set -eu

tools="$(realpath -s "../drivers-tools")"
pushd $tools/.evergreen/github_app || exit

owner="mongodb"
repo="mongo-python-driver"

# Bootstrap the app.
echo "bootstrapping"
source utils.sh
bootstrap drivers/comment-bot

# Run the app.
source ./secrets-export.sh

# Get a github access token for the git checkout.
echo "Getting github token..."

token=$(bash ./get-access-token.sh $repo $owner)
if [ -z "${token}" ]; then
    echo "Failed to get github access token!"
    popd || exit
    exit 1
fi
echo "Getting github token... done."
popd || exit

# Make the git checkout and create a new branch.
echo "Creating the git checkout..."
branch="spec-resync-"$(date '+%m-%d-%Y')

git remote set-url origin https://x-access-token:${token}@github.com/$owner/$repo.git
git checkout -b $branch "origin/main"

# Attribute the commit to the bot user instead of the Evergreen host user.
# The noreply email follows GitHub's standard <id>+<login> form so the commit
# is linked to the bot account on GitHub. Cosmetic: fall back silently to the
# host identity if the lookup fails.
bot_login="mongodb-drivers-pr-bot[bot]"
bot_id=$(curl -gsm 10 "https://api.github.com/users/${bot_login}" | jq -r '.id // empty' || true)
if [ -n "$bot_id" ]; then
    git config user.name "$bot_login"
    git config user.email "${bot_id}+${bot_login}@users.noreply.github.com"
fi

git add ./test
git commit -am "resyncing specs $(date '+%m-%d-%Y')"
echo "Creating the git checkout... done."

# Force-push: the branch name is date-stamped, so any existing remote branch
# is an artifact of an earlier same-day attempt and is always reproducible
# from origin/main plus a fresh commit. This makes same-day retries idempotent.
git push --force origin $branch

# If a PR for this branch already exists (e.g. an earlier same-day attempt),
# reuse it rather than failing to create a duplicate.
existing_pr_url=$(curl -gs \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    --url "https://api.github.com/repos/$owner/$repo/pulls?head=$owner:$branch&state=open" \
    | jq -r '.[0].html_url // empty')
if [ -n "$existing_pr_url" ]; then
    echo "$existing_pr_url"
    echo "Creating the PR... done. (PR already existed; branch was force-pushed)"
    rm -rf $tools
    exit 0
fi

# Build the payload as a file so the body content is always properly
# JSON-escaped, rather than interpolated into the command line as text.
payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
jq -n \
    --arg title "[Spec Resync] $(date '+%m-%d-%Y')" \
    --arg head "${branch}" \
    --rawfile body "$1" \
    '{title: $title, body: $body, head: $head, base: "main"}' > "$payload_file"

resp=$(curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -d "@${payload_file}" \
    --url https://api.github.com/repos/$owner/$repo/pulls)

pr_url=$(echo "$resp" | jq -r '.html_url // empty')
if [ -z "$pr_url" ]; then
    echo "Failed to create PR! API response:"
    echo "$resp" | jq .
    exit 1
fi
echo "$pr_url"
echo "Creating the PR... done."

rm -rf $tools
