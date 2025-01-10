# See https://just.systems/man/en/ for instructions
set shell := ["bash", "-c"]
set dotenv-load
set dotenv-filename := "./.evergreen/scripts/env.sh"

# Make the default recipe private so it doesn't show up in the list.
[private]
default:
  @just --list

install:
    bash .evergreen/scripts/ensure-hatch.sh

[group('docs')]
docs:
    source .evergreen/scripts/ensure-hatch.sh && hatch run doc:build

[group('docs')]
docs-serve:
    source .evergreen/scripts/ensure-hatch.sh && hatch run doc:serve

[group('docs')]
docs-linkcheck:
    hatch run doc:linkcheck

[group('docs')]
docs-test:
    source .evergreen/scripts/ensure-hatch.sh && hatch run doctest:test

[group('typing')]
typing:
    source .evergreen/scripts/ensure-hatch.sh && hatch run typing:check

[group('typing')]
typing-mypy:
    source .evergreen/scripts/ensure-hatch.sh && hatch run typing:mypy

[group('lint')]
lint:
    pre-commit run --all-files

[group('lint')]
lint-manual:
    pre-commit run --all-files --hook-stage manual

[group('test')]
test *args:
    source .evergreen/scripts/ensure-hatch.sh && hatch run test:test {{args}}

[group('test')]
test-mockupdb:
    source .evergreen/scripts/ensure-hatch.sh && hatch run test:test-mockupdb

[group('test')]
test-eg *args:
    source .evergreen/scripts/ensure-hatch.sh && hatch run test:test-eg {{args}}

[group('encryption')]
setup-encryption:
    bash .evergreen/setup-encryption.sh

[group('encryption')]
teardown-encryption:
    bash .evergreen/teardown-encryption.sh
