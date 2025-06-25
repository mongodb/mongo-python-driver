#!/usr/bin/env bash

tools="../drivers-evergreen-tools"
git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git $tools
body="$(cat "$1")"

pushd $tools/.evergreen/github_app

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
    popd
    exit 1
fi
echo "Getting github token... done."
popd

# Make the git checkout and create a new branch.
echo "Creating the git checkout..."
branch="spec-resync-"$(date '+%m-%d-%Y')

#git config user.email "167856002+mongodb-dbx-release-bot[bot]@users.noreply.github.com"
#git config user.name "mongodb-dbx-release-bot[bot]"
git remote set-url origin https://x-access-token:${token}@github.com/$owner/$repo.git
git checkout -b $branch "origin/master"
git add ./test
git apply -R .evergreen/specs.patch
git commit -am "resyncing specs test?"
echo "Creating the git checkout... done."

echo "THIS IS THE BODY"
echo "$body"
git push origin $branch
echo "{\"title\":\"[Spec Resync] $(date '+%m-%d-%Y')\",\"body\":\"$(cat "$1")\",\"head\":\"${branch}\",\"base\":\"master\"}"
resp=$(curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    -d "{\"title\":\"[Spec Resync] $(date '+%m-%d-%Y')\",\"body\":\"$(cat "$1")\",\"head\":\"${branch}\",\"base\":\"master\"}" \
    --url https://api.github.com/repos/$owner/$repo/pulls)
echo $resp
echo $resp | jq '.html_url'
echo "Creating the PR... done."

rm -rf $tools

# use file names or reg-ex patterns
# or automate which version of the spec we support (like schema version)
