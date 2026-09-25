#!/bin/bash
# Set up, run, or tear down the mod_wsgi tests. On Linux the test runs against
# the host's Apache; on other hosts it runs in an ubuntu:24.04 container with
# its own MongoDB replica set.
#
# Usage: mod_wsgi.sh setup <standalone|embedded> | test | teardown | active
set -euo pipefail

SCRIPT_DIR=$(dirname "${BASH_SOURCE:-$0}")
ROOT=$(dirname "$(dirname "$SCRIPT_DIR")")
CMD="${1:-}"
MODE="${2:-}"
CONTAINER=pymongo-mod-wsgi
HOME_SRC=/home/smoke/src
# The venv, MongoDB install, and the recorded version selection live outside
# the source copy, so refreshing the checkout never reinstalls them.
VENV=/home/smoke/venv
VERSIONS=/home/smoke/work/versions

cd "$ROOT"

case "$CMD" in
  setup)
    if [ "$MODE" != "standalone" ] && [ "$MODE" != "embedded" ]; then
      echo "Usage: mod_wsgi.sh setup <standalone|embedded>"
      exit 1
    fi
    # The harness recipes (run-tests, teardown-tests) dispatch on this.
    echo "export TEST_NAME=\"mod_wsgi\"" > .evergreen/scripts/test-env.sh
    echo "export SUB_TEST_NAME=\"$MODE\"" >> .evergreen/scripts/test-env.sh
    # A failed setup leaves a partially started session behind. While the
    # marker still routes to this session, tear it down; then remove the
    # marker, so later just recipes do not route to the half-set-up state.
    # The teardown is what stops the server or container setup may have
    # started, and the next setup attempt stops any leftovers as well.
    cleanup_marker() {
      status=$?
      if [ "$status" -ne 0 ]; then
        bash "$SCRIPT_DIR/mod_wsgi.sh" teardown || true
        rm -f .evergreen/scripts/test-env.sh
      fi
    }
    trap cleanup_marker EXIT
    ;;
  test|teardown|active) ;;
  *)
    echo "Usage: mod_wsgi.sh setup <standalone|embedded> | test | teardown | active"
    exit 1
    ;;
esac

if [ "$CMD" = "active" ]; then
  # Exit 0 when a mod_wsgi session is active in this environment, so the
  # dispatch script can route to us without a stale marker. Uses the system
  # python and the tester's own STATE_FILE constant, so a missing or
  # mismatched uv cannot break the dispatch and the path cannot drift.
  if [ "$(uname -s)" = "Linux" ] && [ -z "${MOD_WSGI_DOCKER:-}" ]; then
    if python3 -c "
import sys
sys.path.insert(0, '.evergreen/scripts')
from mod_wsgi_tester import STATE_FILE
sys.exit(0 if STATE_FILE.exists() else 1)
"; then
      exit 0
    fi
    exit 1
  fi
  # The docker branch below handles the container case.
fi

if [ "$(uname -s)" = "Linux" ] && [ -z "${MOD_WSGI_DOCKER:-}" ]; then
  # Native: Apache runs on the host; MongoDB is already running, started by
  # "just run-server" or the CI workflow. Set MOD_WSGI_DOCKER to force the
  # container path on Linux.
  if [ "$CMD" != "teardown" ]; then
    # Other recipes' exact uv syncs — including run-tests' resync — prune the
    # mod_wsgi group; restore it before setup or test when missing. Checking
    # first keeps the sync from reconciling dependencies that a caller
    # resolved differently, like the min-deps job's lowest-direct install.
    if ! uv run --no-sync python -c "import mod_wsgi" 2>/dev/null; then
      uv sync --group mod_wsgi --quiet
    fi
  fi
  if [ "$CMD" = "setup" ]; then
    uv run --no-sync python .evergreen/scripts/mod_wsgi_tester.py setup "$MODE"
  else
    uv run --no-sync python .evergreen/scripts/mod_wsgi_tester.py "$CMD"
  fi
  # Clear the dispatch marker so later just recipes use the harness.
  if [ "$CMD" = "teardown" ]; then
    rm -f .evergreen/scripts/test-env.sh
  fi
  exit 0
fi

# Docker path for hosts that cannot run Apache and mongod natively.
container_exists() { docker container inspect "$CONTAINER" >/dev/null 2>&1; }
container_running() {
  [ "$(docker container inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" = "true" ]
}

if [ "$CMD" = "active" ]; then
  if container_exists; then
    exit 0
  fi
  exit 1
fi

# Copy the checkout into the container's home directory, so root-owned test
# artifacts never pollute it.
sync_source() {
  docker exec "$CONTAINER" rm -rf "$HOME_SRC"
  docker exec -u smoke "$CONTAINER" mkdir -p "$HOME_SRC"
  tar -C "$ROOT" -cf - --exclude=.git --exclude=.venv \
    --exclude=access_log --exclude=error_log . | docker exec -i -u smoke "$CONTAINER" tar -C "$HOME_SRC" -xf -
}

# Start a single-node replica set, if it is not already running.
ensure_mongod() {
  docker exec --user smoke -w "$HOME_SRC" "$CONTAINER" bash -c 'set -euo pipefail
    export PATH="$HOME/.local/bin:$PATH"
    mkdir -p "$HOME/work/db"
    if ! pgrep -x mongod >/dev/null; then
      "$HOME/work/bin/mongod" --replSet rs0 --bind_ip 127.0.0.1 --port 27017 --dbpath "$HOME/work/db" --fork --logpath "$HOME/work/mongod.log"
    fi
  '
}

# Create the container and install the environment (idempotent).
bootstrap() {
  if ! container_exists; then
    docker run --name "$CONTAINER" -d ubuntu:24.04 sleep infinity
    docker exec "$CONTAINER" apt-get update -qq
    docker exec "$CONTAINER" apt-get install -y -qq apache2 apache2-dev build-essential curl jq git tar gzip ca-certificates
    docker exec "$CONTAINER" useradd -m smoke
  fi
  if ! container_running; then
    docker start "$CONTAINER"
  fi
  # Always refresh the checkout copy, so reused containers test the current
  # source. The venv and versions file live outside it and survive the
  # refresh.
  sync_source
  # Re-evaluate the newest Python and MongoDB on every bootstrap, so a reused
  # container tracks matrix and server releases, but only reinstall when the
  # recorded selection differs, since the downloads are slow.
  docker exec --user smoke -w "$HOME_SRC" \
    -e VERSIONS="$VERSIONS" -e VENV="$VENV" "$CONTAINER" bash -c 'set -euo pipefail
    export PATH="$HOME/.local/bin:$PATH"
    export UV_PYTHON_INSTALL_DIR="$HOME/uv-python"
    export UV_PROJECT_ENVIRONMENT="$VENV"
    if ! command -v uv >/dev/null; then
      curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null
    fi
    # Mirror the CI jobs and test the newest supported CPython.
    LATEST_PYTHON=$(uv run --no-project --with "shrub.py>=3.10.0" python .evergreen/scripts/mod_wsgi_matrix.py | jq -r ".[-1].\"python-version\"")
    case "$(uname -m)" in
      aarch64|arm64) MARCH=aarch64 ;;
      x86_64|amd64) MARCH=x86_64 ;;
      *) echo "Unsupported architecture: $(uname -m)"; exit 1 ;;
    esac
    # MongoDB builds are per-distro; the container is ubuntu:24.04.
    MONGODB_URL=$(curl -fsSL https://downloads.mongodb.org/current.json | jq -r "
      .versions[] | select(.current and .production_release) | .downloads[] |
      select(.target==\"ubuntu2404\" and .arch==\"$MARCH\") | .archive.url" | grep -v enterprise | head -1)
    DESIRED="python=$LATEST_PYTHON mongodb=$MONGODB_URL"
    if [ "$(cat "$VERSIONS" 2>/dev/null || true)" != "$DESIRED" ]; then
      uv python install "$LATEST_PYTHON" >/dev/null
      mkdir -p "$HOME/work/bin"
      curl -fsSL "$MONGODB_URL" -o "$HOME/work/mongo.tgz"
      MEMBER=$(tar -tzf "$HOME/work/mongo.tgz" | grep "bin/mongod$")
      # Strip the two leading path components, so ensure_mongod finds the
      # binary at "$HOME/work/bin/mongod".
      tar -xz -C "$HOME/work/bin" --strip-components=2 -f "$HOME/work/mongo.tgz" "$MEMBER"
      # A new interpreter needs a fresh venv; the sync below repopulates it.
      rm -rf "$VENV"
      printf "%s\n" "$DESIRED" > "$VERSIONS"
    fi
    # Rebuild the in-place C extensions on every bootstrap; the source
    # refresh may carry artifacts built by a different interpreter.
    export PYMONGO_C_EXT_MUST_BUILD=1
    uv sync --group mod_wsgi --refresh-package pymongo
  '
  ensure_mongod
  docker exec --user smoke -w "$HOME_SRC" "$CONTAINER" bash -c 'set -euo pipefail
    export PATH="$HOME/.local/bin:$PATH"
    export UV_PROJECT_ENVIRONMENT="'"$VENV"'"
    if ! uv run --no-sync python -c "from pymongo import MongoClient; MongoClient().admin.command(\"hello\")" 2>/dev/null; then
      uv run --no-sync python -c "
from pymongo import MongoClient
client = MongoClient(\"127.0.0.1:27017\", directConnection=True)
client.admin.command(\"replSetInitiate\", {\"_id\": \"rs0\", \"members\": [{\"_id\": 0, \"host\": \"127.0.0.1:27017\"}]})
"
    fi
  '
}

# Bring a running session to readiness for a test run without refreshing the
# checkout copy: Apache may be live in it, and sync_source would unlink its
# pid file and error log. Only a missing, stopped, or never-bootstrapped
# container needs the full bootstrap, since then nothing is live to disturb.
ensure_ready() {
  if container_exists && container_running \
    && docker exec "$CONTAINER" test -e "$VERSIONS"; then
    ensure_mongod
  else
    bootstrap
  fi
}

case "$CMD" in
  setup)
    bootstrap
    docker exec --user smoke -w "$HOME_SRC" "$CONTAINER" bash -c 'set -euo pipefail
      export PATH="$HOME/.local/bin:$PATH"
      export UV_PYTHON_INSTALL_DIR="$HOME/uv-python"
      export UV_PROJECT_ENVIRONMENT="'"$VENV"'"
      uv run --no-sync python .evergreen/scripts/mod_wsgi_tester.py setup '"$MODE"'
    '
    ;;
  test)
    ensure_ready
    docker exec --user smoke -w "$HOME_SRC" "$CONTAINER" bash -c 'set -euo pipefail
      export PATH="$HOME/.local/bin:$PATH"
      export UV_PROJECT_ENVIRONMENT="'"$VENV"'"
      uv run --no-sync python .evergreen/scripts/mod_wsgi_tester.py test
    '
    ;;
  teardown)
    if container_running; then
      docker exec --user smoke -w "$HOME_SRC" "$CONTAINER" bash -c 'set -euo pipefail
        export PATH="$HOME/.local/bin:$PATH"
        export UV_PROJECT_ENVIRONMENT="'"$VENV"'"
        uv run --no-sync python .evergreen/scripts/mod_wsgi_tester.py teardown
      ' || true
      docker stop -t 1 "$CONTAINER"
    fi
    rm -f .evergreen/scripts/test-env.sh
    ;;
esac
