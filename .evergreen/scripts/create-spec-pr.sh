#!/usr/bin/env bash

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
git checkout -b $branch "origin/master"
git add ./test
git commit -am "resyncing specs $(date '+%m-%d-%Y')"
echo "Creating the git checkout... done."

git push origin $branch
resp=$(curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -d "{\"title\":\"[Spec Resync] $(date '+%m-%d-%Y')\",\"body\":\"$(cat "$1")\",\"head\":\"${branch}\",\"base\":\"master\"}" \
    --url https://api.github.com/repos/$owner/$repo/pulls)
echo $resp | jq '.html_url'
echo "Creating the PR... done."

rm -rf $tools
