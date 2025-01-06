#!/bin/bash

if [ -z "$1" ]
  then
    echo "Must supply a spawn host URL!"
fi

target=$1
user=${target%@*}

echo "Syncing files to $target..."
rsync -haz -e ssh --exclude '.git' --filter=':- .gitignore' -r . $target:/home/$user/mongo-python-driver
# shellcheck disable=SC2034
fswatch -o . | while read f; do rsync -hazv -e ssh --exclude '.git' --filter=':- .gitignore' -r . $target:/home/$user/mongo-python-driver; done
echo "Syncing files to $target... done."
