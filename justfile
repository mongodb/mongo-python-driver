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

[private]
resync:
 @uv sync --quiet

install:
   bash .evergreen/scripts/setup-dev-env.sh

[group('docs')]
docs: && resync
    {{docs_run}} sphinx-build -W -b html doc {{doc_build}}/html

[group('docs')]
docs-serve: && resync
    {{docs_run}} sphinx-autobuild -W -b html doc --watch ./pymongo --watch ./bson --watch ./gridfs {{doc_build}}/serve

[group('docs')]
docs-linkcheck: && resync
    {{docs_run}} sphinx-build -E -b linkcheck doc {{doc_build}}/linkcheck

[group('typing')]
typing: && resync
    just typing-mypy
    just typing-pyright

[group('typing')]
typing-mypy: && resync
    {{typing_run}} python -m mypy {{mypy_args}} bson gridfs tools pymongo
    {{typing_run}} python -m mypy {{mypy_args}} --config-file mypy_test.ini test
    {{typing_run}} python -m mypy {{mypy_args}} test/test_typing.py test/test_typing_strict.py

[group('typing')]
typing-pyright: && resync
    {{typing_run}} python -m pyright test/test_typing.py test/test_typing_strict.py
    {{typing_run}} python -m pyright -p strict_pyrightconfig.json test/test_typing_strict.py

[group('lint')]
lint *args="": && resync
    uvx pre-commit run --all-files {{args}}

[group('lint')]
lint-manual *args="": && resync
    uvx pre-commit run --all-files --hook-stage manual {{args}}

[group('test')]
test *args="-v --durations=5 --maxfail=10": && resync
    uv run --extra test python -m pytest {{args}}

[group('test')]
test-numpy: && resync
    uv run --extra test --with numpy python -m pytest test/test_bson.py

[group('test')]
run-tests *args: && resync
    bash ./.evergreen/run-tests.sh {{args}}

[group('test')]
setup-tests *args="":
    bash .evergreen/scripts/setup-tests.sh {{args}}

[group('test')]
teardown-tests:
    bash .evergreen/scripts/teardown-tests.sh

[group('test')]
integration-tests:
    bash integration_tests/run.sh

[group('server')]
run-server *args="":
    bash .evergreen/scripts/run-server.sh {{args}}

[group('server')]
stop-server:
    bash .evergreen/scripts/stop-server.sh
