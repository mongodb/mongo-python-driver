# See https://just.systems/man/en/ for instructions
set shell := ["bash", "-c"]
set dotenv-load
set dotenv-filename := "./.evergreen/scripts/env.sh"

# Handle cross-platform paths to local python cli tools.
python_bin_dir := if os_family() == "windows" { "./.venv/Scripts" } else { "./.venv/bin" }
hatch_bin := python_bin_dir + "/hatch"
pre_commit_bin := python_bin_dir + "/pre-commit"

# Make the default recipe private so it doesn't show up in the list.
[private]
default:
  @just --list

install:
   bash .evergreen/scripts/setup-dev-env.sh

[group('docs')]
docs:
    {{hatch_bin}} run doc:build

[group('docs')]
docs-serve:
    {{hatch_bin}} run doc:serve

[group('docs')]
docs-linkcheck:
    {{hatch_bin}} run doc:linkcheck

[group('docs')]
docs-test:
    {{hatch_bin}} run doctest:test

[group('typing')]
typing:
    {{hatch_bin}} run typing:check

[group('typing')]
typing-mypy:
    {{hatch_bin}} run typing:mypy

[group('lint')]
lint:
    {{pre_commit_bin}} run --all-files

[group('lint')]
lint-manual:
    {{pre_commit_bin}} run --all-files --hook-stage manual

[group('test')]
test *args:
    {{hatch_bin}} run test:test {{args}}

[group('test')]
test-mockupdb:
    {{hatch_bin}} run test:test-mockupdb

[group('test')]
test-eg *args:
    {{hatch_bin}} run test:test-eg {{args}}

[group('encryption')]
setup-encryption:
    bash .evergreen/setup-encryption.sh

[group('encryption')]
teardown-encryption:
    bash .evergreen/teardown-encryption.sh
