# See https://just.systems/man/en/ for instructions
set shell := ["bash", "-c"]
set dotenv-load
set dotenv-filename := "./.evergreen/scripts/env.sh"

# Commonly used command segments.
uv_run := "uv run --isolated"
typing_run := uv_run + "--group typing --all-extras"
docs_run := uv_run + "--extra docs"
doc_build := "./doc/_build"
mypy_args := "--install-types --non-interactive"

# Make the default recipe private so it doesn't show up in the list.
[private]
default:
  @just --list

install:
   bash .evergreen/scripts/setup-dev-env.sh

[group('docs')]
docs:
    {{docs_run}} sphinx-build -W -b html doc {{doc_build}}/html

[group('docs')]
docs-serve:
    {{docs_run}} sphinx-autobuild -W -b html doc --watch ./pymongo --watch ./bson --watch ./gridfs {{doc_build}}/serve

[group('docs')]
docs-linkcheck:
    {{docs_run}} sphinx-build -E -b linkcheck doc {{doc_build}}/linkcheck

[group('docs')]
docs-test:
    {{docs_run}} --extra test sphinx-build -E -b doctest doc {{doc_build}}/doctest

[group('typing')]
typing:
    just typing-mypy
    just typing-pyright

[group('typing')]
typing-mypy:
    {{typing_run}} mypy {{mypy_args}} bson gridfs tools pymongo
    {{typing_run}} mypy {{mypy_args}} --config-file mypy_test.ini test
    {{typing_run}} mypy {{mypy_args}} test/test_typing.py test/test_typing_strict.py

[group('typing')]
typing-pyright:
    {{typing_run}} pyright test/test_typing.py test/test_typing_strict.py
    {{typing_run}} pyright -p strict_pyrightconfig.json test/test_typing_strict.py

[group('lint')]
lint:
    {{uv_run}} pre-commit run --all-files

[group('lint')]
lint-manual:
    {{uv_run}} pre-commit run --all-files --hook-stage manual

[group('test')]
test *args="-v --durations=5 --maxfail=10":
    {{uv_run}} --extra test pytest {{args}}

[group('test')]
test-mockupdb *args:
    {{uv_run}} -v --extra test --group mockupdb pytest -m mockupdb {{args}}

[group('test')]
test-eg *args:
    bash ./.evergreen/run-tests.sh {{args}}

[group('encryption')]
setup-encryption:
    bash .evergreen/setup-encryption.sh

[group('encryption')]
teardown-encryption:
    bash .evergreen/teardown-encryption.sh
