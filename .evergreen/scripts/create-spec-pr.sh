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

# Attribute the commit to the bot user; fall back to the host identity.
bot_login="mongodb-drivers-pr-bot[bot]"
bot_id=$(curl -gsm 10 "https://api.github.com/users/${bot_login}" | jq -r '.id // empty' || true)
if [ -n "$bot_id" ]; then
    git config user.name "$bot_login"
    git config user.email "${bot_id}+${bot_login}@users.noreply.github.com"
fi

git add ./test
git commit -am "resyncing specs $(date '+%m-%d-%Y')"
echo "Creating the git checkout... done."

# Date-stamped branch: force-push makes same-day reruns idempotent; retry
# absorbs GitHub's intermittent 403s on the receive-pack endpoint.
push_ok=false
for _attempt in 1 2 3; do
    if git push --force origin $branch; then
        push_ok=true
        break
    fi
    if [ "$_attempt" -lt 3 ]; then
        echo "Push attempt ${_attempt} failed; retrying in 30s..."
        sleep 30
    fi
done
if [ "$push_ok" != true ]; then
    echo "Failed to push $branch after 3 attempts!" >&2
    exit 1
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

# Reuse an open PR for this branch (same-day rerun) instead of duplicating it.
existing_pr_json=$(curl -gs \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    --url "https://api.github.com/repos/$owner/$repo/pulls?head=$owner:$branch&state=open")
existing_pr_url=$(echo "$existing_pr_json" | jq -r '.[0].html_url // empty')
if [ -n "$existing_pr_url" ]; then
    existing_pr_number=$(echo "$existing_pr_json" | jq -r '.[0].number // empty')
    if ! curl -sfX PATCH \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer $token" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -d "{\"title\": \"[Spec Resync] $(date '+%m-%d-%Y')\", \"body\": $(jq -Rs . < "$1")}" \
        --url "https://api.github.com/repos/$owner/$repo/pulls/${existing_pr_number}" > /dev/null
    then
        echo "Failed to update PR summary!" >&2
        exit 1
    fi
    echo "$existing_pr_url"
    echo "Creating the PR... done. (PR already existed; branch and summary were updated)"
    rm -rf $tools
    exit 0
fi

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
