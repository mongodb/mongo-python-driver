#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $HERE/env.sh ]; then
  echo "Sourcing env file"
  source $HERE/env.sh
fi

set -x
ls /Library/Frameworks/Python.Framework
ls /Library/Frameworks/Python.Framework/Versions
/Library/Frameworks/Python.Framework/Versions/3.9/bin/python3 -c "import platform;print(platform.machine())"
/Library/Frameworks/Python.Framework/Versions/3.9/bin/python3 -c "import platform;print(platform.processor())"
exit 1
${DRIVERS_TOOLS}/.evergreen/mongodl --out mongodb-bin --strip-path-components 2

./mongodb-bin/mongod --version
exit 1

UV=${UV_BINARY:-uv}
$UV run $HERE/run_server.py "$@"
