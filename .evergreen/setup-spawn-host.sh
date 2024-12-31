#!/bin/bash

set -eu

if [ -z "$1" ]
  then
    echo "Must supply a spawn host URL!"
fi

target=$1

echo "Copying files to $target..."
rsync -az -e ssh --exclude '.git' --filter=':- .gitignore' -r . $target:/home/ec2-user/mongo-python-driver
echo "Copying files to $target... done"

ssh $target /home/ec2-user/mongo-python-driver/.evergreen/scripts/setup-system.sh
