#!/usr/bin/env bash
# Shared plumbing for the automated PR workflows (spec resync and fuzz
# corpus growth): github-app authentication, branch preparation, force
# pushing with retries, and create-or-update against the GitHub API.
#
# Source this file from a wrapper script run from the repository root
# (where the drivers-evergreen-tools submodule lives); it is not meant to
# be executed directly. Defines OWNER, REPO and the functions below;
# requires curl, jq and git.

OWNER="mongodb"
REPO="mongo-python-driver"

# Authenticate with the github app in drivers-evergreen-tools and set
# GITHUB_TOKEN. Exits the caller if no token can be obtained.
acquire_github_token() {
    # Default to the drivers-evergreen-tools submodule when unset; a
    # caller-provided DRIVERS_TOOLS wins, as in the other consumers.
    local tools
    tools="$(realpath -s "${DRIVERS_TOOLS:-./drivers-evergreen-tools}")"
    pushd "$tools/.evergreen/github_app" || exit

    # Bootstrap the app.
    echo "bootstrapping"
    # shellcheck disable=SC1091
    source utils.sh
    bootstrap drivers/comment-bot

    # Run the app.
    # shellcheck disable=SC1091
    source ./secrets-export.sh

    # Get a github access token for the git checkout.
    echo "Getting github token..."

    GITHUB_TOKEN=$(bash ./get-access-token.sh "$REPO" "$OWNER")
    if [ -z "${GITHUB_TOKEN}" ]; then
        echo "Failed to get github access token!"
        popd || exit
        exit 1
    fi
    echo "Getting github token... done."
    popd || exit
}

# Prepare the git checkout: point origin at the repo with token auth,
# check out <branch> fresh from origin/main (-B resets a reused checkout,
# keeping reruns idempotent), and attribute commits to the drivers PR bot
# when its GitHub id is resolvable.
prepare_bot_branch() {
    local branch="$1"

    git remote set-url origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${OWNER}/${REPO}.git"
    git checkout -B "$branch" "origin/main"

    local bot_login="mongodb-drivers-pr-bot[bot]" bot_id
    bot_id=$(curl -gsm 10 "https://api.github.com/users/${bot_login}" | jq -r '.id // empty' || true)
    if [ -n "$bot_id" ]; then
        git config user.name "$bot_login"
        git config user.email "${bot_id}+${bot_login}@users.noreply.github.com"
    fi
}

# Stage the given paths and commit them. Arguments: the commit message,
# then the paths to stage. Returns 1 when the tree was already clean, so
# the caller can skip PR creation. Git failures exit the caller: errexit
# is suspended while this function runs in an "if" condition, so every
# fallible command is guarded explicitly.
stage_and_commit() {
    local message="$1"
    shift

    git add "$@" || exit
    if git diff --cached --quiet && git diff --quiet --ignore-submodules=dirty; then
        return 1
    fi
    git commit -m "$message" || exit
}

# Push <branch> to origin. Force-pushing makes same-day reruns of a
# date-stamped branch idempotent; the retries absorb GitHub's
# intermittent 403s on the receive-pack endpoint. Exits the caller after
# 3 failed attempts.
push_branch() {
    local branch="$1"

    local push_ok=false
    local attempt
    for attempt in 1 2 3; do
        if git push --force origin "$branch"; then
            push_ok=true
            break
        fi
        if [ "$attempt" -lt 3 ]; then
            echo "Push attempt ${attempt} failed; retrying in 30s..."
            sleep 30
        fi
    done
    if [ "$push_ok" != true ]; then
        echo "Failed to push $branch after 3 attempts!"
        exit 1
    fi
}

# Create a PR from <branch>, or, when an open PR is already attached to
# that branch (a same-day rerun), update its title and body instead.
# Arguments: the branch name, the PR title, and the path to a file with
# the PR body. Echoes the PR url and exits the caller if GitHub rejects
# the request.
create_or_update_pr() {
    local branch="$1" title="$2" summary_file="$3"

    # Build the payload as a file so the body content is always properly
    # JSON-escaped, rather than interpolated into the command line as text.
    # Not local: the EXIT trap below must still see it after this function
    # returns.
    payload_file=$(mktemp)
    trap 'rm -f "$payload_file"' EXIT
    jq -n \
        --arg title "$title" \
        --arg head "${branch}" \
        --rawfile body "$summary_file" \
        '{title: $title, body: $body, head: $head, base: "main"}' > "$payload_file"

    # Reuse an open PR for this branch (same-day rerun) instead of duplicating it.
    local existing_pr_json existing_pr_url existing_pr_number
    existing_pr_json=$(curl -gs \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer $GITHUB_TOKEN" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        --url "https://api.github.com/repos/$OWNER/$REPO/pulls?head=$OWNER:$branch&state=open")
    existing_pr_url=$(echo "$existing_pr_json" | jq -r '.[0].html_url // empty')
    if [ -n "$existing_pr_url" ]; then
        existing_pr_number=$(echo "$existing_pr_json" | jq -r '.[0].number // empty')
        if ! curl -sfX PATCH \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer $GITHUB_TOKEN" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            -d "$(jq -n --arg title "$title" --rawfile body "$summary_file" '{title: $title, body: $body}')" \
            --url "https://api.github.com/repos/$OWNER/$REPO/pulls/${existing_pr_number}" > /dev/null
        then
            echo "Failed to update PR summary!"
            exit 1
        fi
        echo "$existing_pr_url"
        echo "Creating the PR... done. (PR already existed; branch and summary were updated)"
        return 0
    fi

    local resp pr_url
    resp=$(curl -L \
        -X POST \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer $GITHUB_TOKEN" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -d "@${payload_file}" \
        --url "https://api.github.com/repos/$OWNER/$REPO/pulls")

    pr_url=$(echo "$resp" | jq -r '.html_url // empty')
    if [ -z "$pr_url" ]; then
        echo "Failed to create PR! API response:"
        echo "$resp" | jq .
        exit 1
    fi
    echo "$pr_url"
    echo "Creating the PR... done."
}
