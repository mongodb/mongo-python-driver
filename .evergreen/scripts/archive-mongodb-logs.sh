#!/bin/bash

set -o xtrace
mkdir out_dir
# shellcheck disable=SC2156
find "$MONGO_ORCHESTRATION_HOME" -name \*.log -exec sh -c 'x="{}"; mv $x $PWD/out_dir/$(basename $(dirname $x))_$(basename $x)' \;
tar zcvf mongodb-logs.tar.gz -C out_dir/ .
rm -rf out_dir
