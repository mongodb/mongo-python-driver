#!/usr/bin/env bash

# clone drivers-evergreen-tools to get this get-access-token script...
tools="../drivers-evergreen-tools"
git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git $tools
body="$(cat "$1")"

pushd $tools/.evergreen/github_app

# Get a github access token for the git checkout.
echo "Getting github token..."

owner="mongodb"
repo="mongo-python-driver"

./setup-secrets.sh
echo "finish setting up secrets"
token=$(bash ./get-access-token.sh $repo $owner)
if [ -z "${token}" ]; then
    echo "Failed to get github access token!"
    popd
    exit 1
fi
echo "Getting github token... done."

# Make the git checkout and create a new branch.
echo "Creating the git checkout..."
branch="spec-resync-"$(date '+%m-%d-%Y')

git config user.email "167856002+mongodb-dbx-release-bot[bot]@users.noreply.github.com"
git config user.name "mongodb-dbx-release-bot[bot]"
git remote set-url origin https://x-access-token:${token}@github.com/$owner/$repo.git
git checkout -b $branch "origin/master"
git add
echo "Creating the git checkout... done."

git push origin $branch
resp=$(curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -d "{\"title\":\"[Spec Resync] $(date '+%m-%d-%Y')\",\"body\":\"${body}\",\"head\":\"${branch}\",\"base\":\"main\"}" \
    --url https://api.github.com/repos/$owner/$repo/pulls)
echo $resp | jq '.html_url'
echo "Creating the PR... done."

rm -rf $dirname
