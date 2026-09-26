# See https://just.systems/man/en/ for instructions
set shell := ["bash", "-c"]

# Commonly used command segments.
typing_run := "uv run --group typing --extra aws --extra encryption --with numpy --extra ocsp --extra snappy --extra test --extra zstd"
docs_run := "uv run --extra docs"
doc_build := "./doc/_build"
mypy_args := "--install-types --non-interactive"

# Make the default recipe private so it doesn't show up in the list.
[private]
default:
  @just --list

# Restore the default development environment. Set JUST_NO_RESYNC to skip the
# sync when a caller resolves dependencies differently, such as the
# minimum-dependencies job in test-mod-wsgi.yml.
[private]
resync:
 @if [ -z "${JUST_NO_RESYNC:-}" ]; then uv sync --quiet; fi

# Set up the development environment
install:
   bash .evergreen/scripts/setup-dev-env.sh

# Build the HTML documentation
[group('docs')]
docs: && resync
    {{docs_run}} sphinx-build -W -b html doc {{doc_build}}/html

# Serve the docs locally with live-reload
[group('docs')]
docs-serve: && resync
    {{docs_run}} sphinx-autobuild -W -b html doc --watch ./pymongo --watch ./bson --watch ./gridfs {{doc_build}}/serve

# Check documentation hyperlinks for broken URLs
[group('docs')]
docs-linkcheck: && resync
    {{docs_run}} sphinx-build -E -b linkcheck doc {{doc_build}}/linkcheck

# Run mypy and pyright
[group('typing')]
typing: && resync
    just typing-mypy
    just typing-pyright

# Run mypy against the library source and test suite
[group('typing')]
typing-mypy: && resync
    {{typing_run}} python -m mypy {{mypy_args}} bson gridfs tools pymongo
    {{typing_run}} python -m mypy {{mypy_args}} --config-file mypy_test.ini test
    {{typing_run}} python -m mypy {{mypy_args}} test/test_typing.py test/test_typing_strict.py

# Run pyright against the typing test files
[group('typing')]
typing-pyright: && resync
    {{typing_run}} python -m pyright test/test_typing.py test/test_typing_strict.py
    {{typing_run}} python -m pyright -p strict_pyrightconfig.json test/test_typing_strict.py

# Run all pre-commit hooks across the repository
[group('lint')]
lint *args="": && resync
    uvx pre-commit run --all-files {{args}}

# Run shellcheck, doc8, and slotscheck
[group('lint')]
lint-manual *args="": && resync
    uvx pre-commit run --all-files --hook-stage manual {{args}}

# Run synchro
[group('lint')]
synchro: && resync
    uvx pre-commit run --all-files synchro

# Run pytest (e.g. just test test/test_uri_parser.py)
[group('test')]
test *args="-v --durations=5 --maxfail=10": && resync
    #!/usr/bin/env bash
    set -euo pipefail
    uv run ${USE_ACTIVE_VENV:+--active} --extra test python -m pytest {{args}}

# Run the BSON test suite with numpy
[group('test')]
test-numpy *args="": && resync
    just setup-tests numpy {{args}}
    just run-tests test/test_bson.py

# Run tests via the Evergreen test runner script
[group('test')]
run-tests *args: && resync
    bash .evergreen/scripts/dispatch.sh run-tests {{args}}

# Set up the test environment (auth, TLS, etc.)
[group('test')]
setup-tests *args="":
    bash .evergreen/scripts/dispatch.sh setup-tests {{args}}

# Tear down resources created by setup-tests
[group('test')]
teardown-tests *args="":
    bash .evergreen/scripts/dispatch.sh teardown-tests {{args}}

[group('test')]
integration-tests:
    bash integration_tests/run.sh

# Run the full test suite with coverage
[group('test')]
test-coverage *args="":
    just setup-tests --cov
    just run-tests {{args}}

# Print the coverage summary to the terminal
[group('coverage')]
coverage-report:
    uv tool run --with "coverage[toml]" coverage report

# Generate an HTML coverage report in htmlcov/
[group('coverage')]
coverage-html:
    uv tool run --with "coverage[toml]" coverage html
    @echo "Coverage report generated in htmlcov/index.html"

# Generate an XML coverage report at coverage.xml
[group('coverage')]
coverage-xml:
    uv tool run --with "coverage[toml]" coverage xml
    @echo "Coverage report generated in coverage.xml"

# Run the mod_wsgi tests (natively on Linux, else in an ubuntu container)
[group('test')]
smoke-mod-wsgi:
    #!/usr/bin/env bash
    set -euo pipefail
    STATUS=0
    # Tear down on an early exit (a failed setup or teardown), so a partially
    # started Apache or container and the dispatch marker never outlive the
    # recipe; the per-mode teardown below clears the guard.
    CLEANED=1
    cleanup() {
      [ "$CLEANED" -eq 1 ] || just teardown-tests || true
    }
    trap cleanup EXIT
    for mode in standalone embedded; do
      CLEANED=0
      just setup-tests mod_wsgi $mode
      just run-tests || STATUS=$?
      just teardown-tests
      CLEANED=1
      [ "$STATUS" -eq 0 ] || exit "$STATUS"
    done

# Start a MongoDB server via drivers-evergreen-tools
[group('server')]
run-server *args="":
    bash .evergreen/scripts/run-server.sh {{args}}

[group('server')]
stop-server:
    bash .evergreen/scripts/stop-server.sh
