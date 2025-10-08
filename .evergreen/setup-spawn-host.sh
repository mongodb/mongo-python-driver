#!/bin/bash
# Set up a remote evergreen spawn host.
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
ssh $target "cd $remote_dir && PYTHON_VERSION=${PYTHON_VERSION:-} .evergreen/scripts/setup-uv-python.sh"
ssh $target "cd $remote_dir && .evergreen/scripts/setup-dev-env.sh"
