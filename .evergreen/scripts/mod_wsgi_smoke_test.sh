#!/bin/bash
# Smoke test the mod_wsgi tests in an ubuntu:24.04 container, mirroring the
# Mod WSGI job in .github/workflows/test-python.yml.
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
ROOT=$(dirname "$(dirname "$SCRIPT_DIR")")

# Re-exec the script inside a container when run on the host.
if [ ! -f /.dockerenv ]; then
  if ! command -v docker >/dev/null; then
    echo "docker is required to run the mod_wsgi smoke test"
    exit 1
  fi
  exec docker run --rm -v "$ROOT":/src:ro ubuntu:24.04 bash /src/.evergreen/scripts/mod_wsgi_smoke_test.sh
fi

if [ "$(id -u)" = "0" ]; then
  # Apache and mongod must not run as root. Install system packages, copy the
  # checkout into a home directory, and re-run this script unprivileged.
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq apache2 apache2-dev build-essential curl jq git >/dev/null
  # Install uv to a system path from a pinned release, verifying the published
  # checksum, rather than piping a mutable installer script into a shell.
  UV_VERSION=0.12.17
  case "$(uname -m)" in
    x86_64) UV_TARGET=x86_64-unknown-linux-gnu ;;
    aarch64 | arm64) UV_TARGET=aarch64-unknown-linux-gnu ;;
    *) echo "Unsupported architecture" >&2; exit 1 ;;
  esac
  curl -fsSL -o "/tmp/uv-${UV_TARGET}.tar.gz" "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${UV_TARGET}.tar.gz"
  curl -fsSL -o "/tmp/uv-${UV_TARGET}.tar.gz.sha256" "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${UV_TARGET}.tar.gz.sha256"
  (cd /tmp && sha256sum -c "uv-${UV_TARGET}.tar.gz.sha256")
  tar -xzf "/tmp/uv-${UV_TARGET}.tar.gz" -C /tmp
  install -m 0755 "/tmp/uv-${UV_TARGET}/uv" /usr/local/bin/uv
  rm -rf "/tmp/uv-${UV_TARGET}.tar.gz" "/tmp/uv-${UV_TARGET}.tar.gz.sha256" "/tmp/uv-${UV_TARGET}"
  useradd -m smoke
  mkdir -p /home/smoke/src
  # Leave out the generated env files so they cannot override the versions
  # set below.
  tar -C /src -cf - --exclude=.git --exclude=.venv --exclude=.evergreen/scripts/env.sh \
    --exclude=.evergreen/scripts/test-env.sh . | tar -C /home/smoke/src -xf -
  chown -R smoke:smoke /home/smoke/src
  exec su - smoke -c "bash /src/.evergreen/scripts/mod_wsgi_smoke_test.sh"
fi

export PATH="$HOME/.local/bin:$PATH"
uv tool install rust-just >/dev/null

cd /home/smoke/src

# mongod inherits the soft nofile limit; the 1024 default is exhausted by the
# connection storm the parallel test generates. Best effort: some container
# hosts set a lower hard limit.
ulimit -n 65536 2>/dev/null || true

# Mirror the GHA job and test the newest supported CPython.
LATEST_PYTHON=$(uv run --no-project --with 'shrub.py>=3.10.0' python .evergreen/scripts/mod_wsgi_matrix.py | jq -r '.[-1]."python-version"')
echo "Testing with CPython $LATEST_PYTHON"
uv python install "$LATEST_PYTHON" >/dev/null

export UV_PYTHON=$LATEST_PYTHON
export PYMONGO_C_EXT_MUST_BUILD=1
just install
uv sync --group mod_wsgi

# Start a single-node replica set.
MARCH=$(uname -m)
if [ "$MARCH" != "aarch64" ] && [ "$MARCH" != "x86_64" ]; then
  echo "Unsupported architecture: $MARCH"
  exit 1
fi
MONGODB_URL=$(curl -fsSL https://downloads.mongodb.org/current.json | jq -r "
  .versions[] | select(.current and .production_release) | .downloads[] |
  select(.target==\"ubuntu2404\" and .arch==\"$MARCH\") | .archive.url" | grep -v enterprise | head -1)
curl -fsSL "$MONGODB_URL" -o /tmp/mongo.tgz
MEMBER=$(tar -tzf /tmp/mongo.tgz | grep "bin/mongod$")
tar -xz -C /tmp --strip-components=2 -f /tmp/mongo.tgz "$MEMBER"
mkdir -p /tmp/db
/tmp/mongod --replSet rs0 --bind_ip 127.0.0.1 --port 27017 --dbpath /tmp/db --fork --logpath /tmp/mongod.log
uv run python -c "
from pymongo import MongoClient
client = MongoClient('127.0.0.1:27017', directConnection=True)
client.admin.command('replSetInitiate', {'_id': 'rs0', 'members': [{'_id': 0, 'host': '127.0.0.1:27017'}]})
MongoClient().admin.command('hello')
"

# The mod_wsgi group is part of the synced environment, so both modes can run
# without reinstalling.
for MODE in standalone embedded; do
  bash .evergreen/scripts/setup-tests.sh mod_wsgi $MODE
  bash .evergreen/run-tests.sh
  bash .evergreen/scripts/teardown-tests.sh
done
echo "mod_wsgi smoke test passed"
