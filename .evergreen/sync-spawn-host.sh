#!/bin/bash
# Synchronize local files to a remote Evergreen spawn host.
set -eu

if [ -z "$1" ]
  then
    echo "Must supply a spawn host URL!"
fi

target=$1
user=${target%@*}
remote_dir=/home/$user/mongo-python-driver

echo "Copying files to $target..."
rsync -az -e ssh --exclude '.git' --filter=':- .gitignore' -r . $target:$remote_dir
echo "Copying files to $target... done."
echo "Syncing files to $target..."
# shellcheck disable=SC2034
fswatch -o . | while read f; do rsync -hazv -e ssh --exclude '.git' --filter=':- .gitignore' -r . $target:/home/$user/mongo-python-driver; done
echo "Syncing files to $target... done."
