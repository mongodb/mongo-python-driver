#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

rm -rf mongodb/pymongo
rm -rf mongodb/gridfs
rm -rf mongodb/bson

pushd ../..
rm -f pymongo/*.so
rm -f bson/*.so
image="quay.io/pypa/manylinux2014_x86_64:latest"

DOCKER=$(command -v docker) || true
if [ -n "$DOCKER" ]; then
    PODMAN=$(command -v podman) || true
    if [ -n "$PODMAN" ]; then
        echo "docker or podman are required!"
    fi
    DOCKER=podman
fi

$DOCKER run --rm -v "`pwd`:/src" $image /src/test/lambda/build_internal.sh
cp -r pymongo ./test/lambda/mongodb/pymongo
cp -r bson ./test/lambda/mongodb/bson
cp -r gridfs ./test/lambda/mongodb/gridfs
popd
