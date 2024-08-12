set -x
# Get the current unique version of this checkout
if [ "$is_patch" = "true" ]; then
    CURRENT_VERSION="$(git describe)-patch-$version_id"
else
    CURRENT_VERSION=latest
fi

export DRIVERS_TOOLS="$(dirname $(pwd))/drivers-tools"
export PROJECT_DIRECTORY="$(pwd)"

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home directory
if [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
    export DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
    export PROJECT_DIRECTORY=$(cygpath -m $PROJECT_DIRECTORY)
fi

SCRIPT_DIR="$PROJECT_DIRECTORY/.evergreen/scripts"

if [ -f "$SCRIPT_DIR/env.sh" ]; then
  echo "Reading $SCRIPT_DIR/env.sh file"
  . "$SCRIPT_DIR/env.sh"
  exit 0
fi

cat <<EOT > "$SCRIPT_DIR/env.sh"
export MONGO_ORCHESTRATION_HOME="$DRIVERS_TOOLS/.evergreen/orchestration"
export MONGODB_BINARIES="$DRIVERS_TOOLS/mongodb/bin"

export CURRENT_VERSION="$CURRENT_VERSION"
export DRIVERS_TOOLS="$DRIVERS_TOOLS"
export MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
export MONGODB_BINARIES="$MONGODB_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"

set -o errexit
export SKIP_LEGACY_SHELL=1
export DRIVERS_TOOLS="$DRIVERS_TOOLS"
export MONGO_ORCHESTRATION_HOME="$MONGO_ORCHESTRATION_HOME"
export MONGODB_BINARIES="$MONGODB_BINARIES"
export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"

export TMPDIR="$MONGO_ORCHESTRATION_HOME/db"
export PATH="$MONGODB_BINARIES:$PATH"
export PROJECT="$project"
export PIP_QUIET=1
EOT
