#!/bin/bash

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
echo "Copying files to $target... done"

ssh $target $remote_dir/.evergreen/scripts/setup-system.sh
ssh $target "PYTHON_BINARY=${PYTHON_BINARY:-} $remote_dir/.evergreen/scripts/setup-dev-env.sh"
