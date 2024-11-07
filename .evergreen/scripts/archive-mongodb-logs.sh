#!/bin/bash

. src/.evergreen/scripts/env.sh
set -o xtrace
mkdir out_dir
find "$MONGO_ORCHESTRATION_HOME" -name \*.log -exec sh -c 'x=$1; mv $x $PWD/out_dir/$(basename $(dirname $x))_$(basename $x)' shell {} \;
tar zcvf mongodb-logs.tar.gz -C out_dir/ .
rm -rf out_dir
