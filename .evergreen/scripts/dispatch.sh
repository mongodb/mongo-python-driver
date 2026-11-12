#!/bin/bash
# Route the just test recipes to the mod_wsgi tests or the Evergreen test
# runner. The mod_wsgi setup writes TEST_NAME to the test env file.
set -eu

SCRIPT_DIR=$(dirname "${BASH_SOURCE:-$0}")
RECIPE="${1:-}"
if [ "$#" -gt 0 ]; then
  shift
fi

mod_wsgi_env() {
  [ -f "$SCRIPT_DIR/test-env.sh" ] \
    && grep -q 'TEST_NAME="mod_wsgi"' "$SCRIPT_DIR/test-env.sh" \
    && bash "$SCRIPT_DIR/mod_wsgi.sh" active
}

case "$RECIPE" in
  setup-tests)
    if [ "${1:-}" = "mod_wsgi" ]; then
      shift
      exec bash "$SCRIPT_DIR/mod_wsgi.sh" setup "${1:-}"
    fi
    exec bash "$SCRIPT_DIR/setup-tests.sh" "$@"
    ;;
  run-tests)
    if mod_wsgi_env; then
      if [ "$#" -gt 0 ]; then
        echo "Ignoring arguments for the mod_wsgi tests: $*" >&2
      fi
      exec bash "$SCRIPT_DIR/mod_wsgi.sh" test
    fi
    exec bash "$(dirname "$SCRIPT_DIR")/run-tests.sh" "$@"
    ;;
  teardown-tests)
    if [ -f "$SCRIPT_DIR/test-env.sh" ] \
      && grep -q 'TEST_NAME="mod_wsgi"' "$SCRIPT_DIR/test-env.sh"; then
      exec bash "$SCRIPT_DIR/mod_wsgi.sh" teardown
    fi
    exec bash "$SCRIPT_DIR/teardown-tests.sh" "$@"
    ;;
  *)
    echo "Unknown recipe: $RECIPE"
    exit 1
    ;;
esac
