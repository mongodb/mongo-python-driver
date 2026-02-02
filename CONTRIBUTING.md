# Contributing to PyMongo

PyMongo has a large
[community](https://pymongo.readthedocs.io/en/stable/contributors.html)
and contributions are always encouraged. Contributions can be as simple
as minor tweaks to the documentation. Please read these guidelines
before sending a pull request.

## Bugfixes and New Features

Before starting to write code, look for existing
[tickets](https://jira.mongodb.org/browse/PYTHON) or [create
one](https://jira.mongodb.org/browse/PYTHON) for your specific issue or
feature request. That way you avoid working on something that might not
be of interest or that has already been addressed.

## Supported Interpreters

PyMongo supports CPython 3.9+ and PyPy3.9+. Language features not
supported by all interpreters can not be used.

## Style Guide

PyMongo follows [PEP8](http://www.python.org/dev/peps/pep-0008/)
including 4 space indents and 79 character line limits.

## General Guidelines

-   Avoid backward breaking changes if at all possible.
-   Write inline documentation for new classes and methods.
-   We use [uv](https://docs.astral.sh/uv/) for python environment management and packaging.
-   We use [just](https://just.systems/man/en/) as our task runner.
-   Write tests and make sure they pass (make sure you have a mongod
    running on the default port, then execute `just test` from the cmd
    line to run the test suite).
-   Add yourself to doc/contributors.rst `:)`

## Authoring a Pull Request

**Our Pull Request Policy is based on this** [Code Review Developer
Guide](https://google.github.io/eng-practices/review)

The expectation for any code author is to provide all the context needed
in the space of a pull request for any engineer to feel equipped to
review the code. Depending on the type of change, do your best to
highlight important new functions or objects you've introduced in the
code; think complex functions or new abstractions. Whilst it may seem
like more work for you to adjust your pull request, the reality is your
likelihood for getting review sooner shoots up.

**Self Review Guidelines to follow**

-   If the PR is too large, split it if possible.

    - Use 250 LoC (excluding test data and config changes) as a
            rule-of-thumb.

     - Moving and changing code should be in separate PRs or commits.

        -   Moving: Taking large code blobs and transplanting
            them to another file. There\'s generally no (or very
            little) actual code changed other than a cut and
            paste. It can even be extended to large deletions.
        -   Changing: Adding code changes (be that refactors or
            functionality additions/subtractions).
        -   These two, when mixed, can muddy understanding and
            sometimes make it harder for reviewers to keep track
            of things.

-   Prefer explaining with code comments instead of PR comments.

**Provide background**

-   The PR description and linked tickets should answer the "what" and
    "why" of the change. The code change explains the "how".

**Follow the Template**

-   Please do not deviate from the template we make; it is there for a
    lot of reasons. If it is a one line fix, we still need to have
    context on what and why it is needed.

-   If making a versioning change, please let that be known. See examples below:

    -   `versionadded:: 3.11`
    -   `versionchanged:: 3.5`

**Pull Request Template Breakdown**

-  **Github PR Title**

    -   The PR Title format should always be
            `[JIRA-ID] : Jira Title or Blurb Summary`.

-  **JIRA LINK**

-   Convenient link to the associated JIRA ticket.

-   **Summary**

     -   Small blurb on why this is needed. The JIRA task should have
            the more in-depth description, but this should still, at a
            high level, give anyone looking an understanding of why the
            PR has been checked in.

-    **Changes in this PR**

     -   The explicit code changes that this PR is introducing. This
            should be more specific than just the task name. (Unless the
            task name is very clear).

-   **Test Plan**

    -   Everything needs a test description. Describe what you did
            to validate your changes actually worked; if you did
            nothing, then document you did not test it. Aim to make
            these steps reproducible by other engineers, specifically
            with your primary reviewer in mind.

-   **Screenshots**

    -   Any images that provide more context to the PR. Usually,
            these just coincide with the test plan.

-   **Callouts or follow-up items**

    -   This is a good place for identifying "to-dos" that you've
            placed in the code (Must have an accompanying JIRA Ticket).
    -   Potential bugs that you are unsure how to test in the code.
    -   Opinions you want to receive about your code.

## Running Linters

PyMongo uses [pre-commit](https://pypi.org/project/pre-commit/) for
managing linting of the codebase. `pre-commit` performs various checks
on all files in PyMongo and uses tools that help follow a consistent
code style within the codebase.

To set up `pre-commit` locally, run:

```bash
brew install pre-commit
pre-commit install
```

To run `pre-commit` manually, run:

```bash
pre-commit run --all-files
```

To run a manual hook like `ruff` manually, run:

```bash
pre-commit run --all-files --hook-stage manual ruff
```

Typically we use `just` to run the linters, e.g.

```bash
just install  # this will install a venv with pre-commit installed, and install the pre-commit hook.
just typing-mypy
just run lint-manual
```

## Documentation

To contribute to the [API documentation](https://pymongo.readthedocs.io/en/stable/) just make your
changes to the inline documentation of the appropriate [source code](https://github.com/mongodb/mongo-python-driver) or
[rst file](https://github.com/mongodb/mongo-python-driver/tree/master/doc) in
a branch and submit a [pull request](https://help.github.com/articles/using-pull-requests). You
might also use the GitHub
[Edit](https://github.com/blog/844-forking-with-the-edit-button) button.

We use [reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html) for all
documentation including narrative docs, and the [Sphinx docstring format](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html).

You can build the documentation locally by running:

```bash
just docs
```

When updating docs, it can be helpful to run the live docs server as:

```bash
just docs-serve
```

Browse to the link provided, and then as you make changes to docstrings or narrative docs,
the pages will re-render and the browser will automatically refresh.

## Running Tests Locally

-   Run `just install` to set a local virtual environment, or you can manually
    create a virtual environment and run `pytest` directly.  If you want to use a specific
    version of Python, set `UV_PYTHON` before running `just install`.
-   Ensure you have started the appropriate Mongo Server(s).  You can run `just run-server` with optional args
    to set up the server.  All given options will be passed to
    [`run-orchestration.sh`](https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/run-orchestration.sh).  Run `$DRIVERS_TOOLS/evergreen/run-orchestration.sh -h`
    for a full list of options.
-   Run `just test` or `pytest` to run all of the tests.
-   Append `test/<mod_name>.py::<class_name>::<test_name>` to run
    specific tests. You can omit the `<test_name>` to test a full class
    and the `<class_name>` to test a full module. For example:
    `just test test/test_change_stream.py::TestUnifiedChangeStreamsErrors::test_change_stream_errors_on_ElectionInProgress`.
-   Use the `-k` argument to select tests by pattern.


## Running tests that require secrets, services, or other configuration

### Prerequisites

- Clone `drivers-evergreen-tools`:
    `git clone git@github.com:mongodb-labs/drivers-evergreen-tools.git`.
- Run `export DRIVERS_TOOLS=$PWD/drivers-evergreen-tools`.  This can be put into a `.bashrc` file
  for convenience.
- Some tests require access to [Drivers test secrets](https://github.com/mongodb-labs/drivers-evergreen-tools/tree/master/.evergreen/secrets_handling#secrets-handling).

### Usage

- Run `just run-server` with optional args to set up the server.
- Run `just setup-tests` with optional args to set up the test environment, secrets, etc.
  See `just setup-tests -h` for a full list of available options.
- Run `just run-tests` to run the tests in an appropriate Python environment.
- When done, run `just teardown-tests` to clean up and `just stop-server` to stop the server.

### SSL tests

- Run `just run-server --ssl` to start the server with TLS enabled.
- Run `just setup-tests --ssl`.
- Run `just run-tests`.

Note: for general testing purposes with an TLS-enabled server, you can use the following (this should ONLY be used
for local testing):

```python
from pymongo import MongoClient

client = MongoClient(
    "mongodb://localhost:27017?tls=true&tlsAllowInvalidCertificates=true"
)
```

If you want to use the actual certificate file then set `tlsCertificateKeyFile` to the local path
to `<repo_roo>/test/certificates/client.pem` and `tlsCAFile` to the local path to `<repo_roo>/test/certificates/ca.pem`.

### Encryption tests

- Run `just run-server` to start the server.
- Run `just setup-tests encryption`.
- Run the tests with `just run-tests`.

To test with `encryption` and `PyOpenSSL`, use `just setup-tests encryption pyopenssl`.

### PyOpenSSL tests

- Run `just run-server` to start the server.
- Run `just setup-tests default_sync pyopenssl`.
- Run the tests with `just run-tests`.

Note: `PyOpenSSL` is not used in async tests, but you can use `just setup-tests default_async pyopenssl`
to verify that PyMongo falls back to the standard library `OpenSSL`.

### Load balancer tests

- Install `haproxy` (available as `brew install haproxy` on macOS).
- Start the server with `just run-server load_balancer`.
- Set up the test with `just setup-tests load_balancer`.
- Run the tests with `just run-tests`.

### AWS auth tests

- Run `just run-server auth_aws` to start the server.
- Run `just setup-tests auth_aws <aws-test-type>` to set up the AWS test.
- Run the tests with `just run-tests`.

### OIDC auth tests

- Run `just setup-tests auth_oidc <oidc-test-type>` to set up the OIDC test.
- Run the tests with `just run-tests`.

The supported types are [`default`, `azure`, `gcp`, `eks`, `aks`, and `gke`].
For the `eks` test, you will need to set up access to the `drivers-test-secrets-role`, see the [Wiki](https://wiki.corp.mongodb.com/spaces/DRIVERS/pages/239737385/Using+AWS+Secrets+Manager+to+Store+Testing+Secrets).

### KMS tests

For KMS tests that are run locally, and expected to fail, in this case using `azure`:

- Run `just run-server`.
- Run `just setup-tests kms azure-fail`.
- Run `just run-tests`.

For KMS tests that run remotely and are expected to pass, in this case using `gcp`:

- Run `just setup-tests kms gcp`.
- Run `just run-tests`.

### Enterprise Auth tests

Note: these tests can only be run from an Evergreen host.

- Run `just run-server enterprise_auth`.
- Run `just setup-tests enterprise_auth`.
- Run `just run-tests`.

### Atlas Connect tests

- Run `just setup-tests atlas_connect`.
- Run `just run-tests`.

### Search Index tests

- Run `just run-server search_index`.
- Run `just setup-tests search_index`.
- Run `just run-tests`.

### MockupDB tests

- Run `just setup-tests mockupdb`.
- Run `just run-tests`.

### Doc tests

The doc tests require a running server.

- Run `just run-server`.
- Run `just setup-tests doctest`.
- Run `just run-tests`.

### Free-threaded Python Tests

In the evergreen builds, the tests are configured to use the free-threaded python from the toolchain.
Locally you can run:

- Run `just run-server`.
- Run `just setup-tests`.
- Run `UV_PYTHON=3.14t just run-tests`.

### AWS Lambda tests

You will need to set up access to the `drivers-test-secrets-role`, see the [Wiki](https://wiki.corp.mongodb.com/spaces/DRIVERS/pages/239737385/Using+AWS+Secrets+Manager+to+Store+Testing+Secrets).

- Run `just setup-tests aws_lambda`.
- Run `just run-tests`.

### mod_wsgi tests

Note: these tests can only be run from an Evergreen Linux host that has the Python toolchain.

- Run `just run-server`.
- Run `just setup-tests mod_wsgi <mode>`.
- Run `just run-tests`.

The `mode` can be `standalone` or `embedded`.  For the `replica_set` version of the tests, use
`TOPOLOGY=replica_set just run-server`.

### OCSP tests

- Export the orchestration file, e.g. `export ORCHESTRATION_FILE=rsa-basic-tls-ocsp-disableStapling.json`.
This corresponds to a config file in `$DRIVERS_TOOLS/.evergreen/orchestration/configs/servers`.
MongoDB servers on MacOS and Windows do not staple OCSP responses and only support RSA.
NOTE: because the mock ocsp responder MUST be started prior to the server starting, the ocsp tests start the server
as part of `setup-tests`.

- Run `just setup-tests ocsp <sub test>` (options are "valid", "revoked", "valid-delegate", "revoked-delegate").
- Run `just run-tests`

If you are running one of the `no-responder` tests, omit the `run-server` step.

### Perf Tests

- Start the appropriate server, e.g. `just run-server --version=v8.0-perf --ssl`.
- Set up the tests with `sync` or `async`: `just setup-tests perf sync`.
- Run the tests: `just run-tests`.

## Enable Debug Logs

- Use `-o log_cli_level="DEBUG" -o log_cli=1` with `just test` or `pytest` to output all debug logs to the terminal. **Warning**: This will output a huge amount of logs.
- Add `log_cli=1` and `log_cli_level="DEBUG"` to the `tool.pytest.ini_options` section in `pyproject.toml` to enable debug logs in this manner by default on your machine.
- Set `DEBUG_LOG=1` and run `just setup-tests`, `just-test`, or `pytest` to enable debug logs only for failed tests.
- Finally, you can use `just setup-tests --debug-log`.
- For evergreen patch builds, you can use `evergreen patch --param DEBUG_LOG=1` to enable debug logs for failed tests in the patch.

## Testing minimum dependencies

To run any of the test suites with minimum supported dependencies, pass `--test-min-deps` to
`just setup-tests`.

## Testing time-dependent operations

- `test.utils_shared.delay` - One can trigger an arbitrarily long-running operation on the server using this delay utility
  in combination with a `$where` operation. Use this to test behaviors around timeouts or signals.

## Adding a new test suite

- If adding new tests files that should only be run for that test suite, add a pytest marker to the file and add
  to the list of pytest markers in `pyproject.toml`.  Then add the test suite to the `TEST_SUITE_MAP` in `.evergreen/scripts/utils.py`.  If for some reason it is not a pytest-runnable test, add it to the list of `EXTRA_TESTS` instead.
- If the test uses Atlas or otherwise doesn't use `run-orchestration.sh`, add it to the `NO_RUN_ORCHESTRATION` list in
  `.evergreen/scripts/utils.py`.
- If there is something special required to run the local server or there is an extra flag that should always be set
  like `AUTH`, add that logic to `.evergreen/scripts/run_server.py`.
- The bulk of the logic will typically be in `.evergreen/scripts/setup_tests.py`.  This is where you should fetch secrets and make them available using `write_env`, start services, and write other env vars needed using `write_env`.
- If there are any special test considerations, including not running `pytest` at all, handle it in `.evergreen/scripts/run_tests.py`.
- If there are any services or atlas clusters to teardown, handle them in `.evergreen/scripts/teardown_tests.py`.
- Add functions to generate the test variant(s) and task(s) to the `.evergreen/scripts/generate_config.py`.
- There are some considerations about the Python version used in the test:
    - If a specific version of Python is needed in a task that is running on variants with a toolchain, use
``TOOLCHAIN_VERSION`` (e.g. `TOOLCHAIN_VERSION=3.10`).  The actual path lookup needs to be done on the host, since
tasks are host-agnostic.
    - If a specific Python binary is needed (for example on the FIPS host), set `UV_PYTHON=/path/to/python`.
    - If a specific Python version is needed and the toolchain will not be available, use `UV_PYTHON` (e.g. `UV_PYTHON=3.11`).
    - The default if neither ``TOOLCHAIN_VERSION`` or ``UV_PYTHON`` is set is to use UV to install the minimum
      supported version of Python and use that.  This ensures a consistent behavior across host types that do not
      have the Python toolchain (e.g. Azure VMs), by having a known version of Python with the build headers (`Python.h`)
      needed to build the C extensions.
- Regenerate the test variants and tasks using `pre-commit run --all-files generate-config`.
- Make sure to add instructions for running the test suite to `CONTRIBUTING.md`.

## Handling flaky tests

We have a custom `flaky` decorator in [test/asynchronous/utils.py](test/asynchronous/utils.py) that can be used for
tests that are `flaky`.  By default the decorator only applies when not running on CPython on Linux, since other
runtimes tend to have more variation.  When using the `flaky` decorator, open a corresponding ticket and
a use the ticket number as the "reason" parameter to the decorator, e.g. `@flaky(reason="PYTHON-1234")`.
When running tests locally (not in CI), the `flaky` decorator will be disabled unless `ENABLE_FLAKY` is set.
To disable the `flaky` decorator in CI, you can use `evergreen patch --param DISABLE_FLAKY=1`.

## Integration Tests

The `integration_tests` directory has a set of scripts that verify the usage of PyMongo with downstream packages or frameworks.  See the [README](./integration_tests/README.md) for more information.

To run the tests, use `just integration_tests`.

The tests should be able to run with and without SSL enabled.

## Specification Tests

The MongoDB [specifications repository](https://github.com/mongodb/specifications)
holds in progress and completed specifications for features of MongoDB, drivers,
and associated products. PyMongo supports the [Unified Test Format](https://jira.mongodb.org/browse/DRIVERS-709)
for running specification tests to confirm PyMongo behaves as expected.

### Resynchronizing the Specification Tests

If you would like to re-sync the copy of the specification tests in the
PyMongo repository with that which is inside the [specifications
repo](https://github.com/mongodb/specifications), please use the script
provided in `.evergreen/resync-specs.sh`.:

```bash
git clone git@github.com:mongodb/specifications.git
export MDB_SPECS=~/specifications
cd ~/mongo-python-driver/.evergreen
./resync-specs.sh -b "<regex>" spec1 spec2 ...
./resync-specs.sh -b "connection-string*" crud bson-corpus # Updates crud and bson-corpus specs while ignoring all files with the regex "connection-string*"
cd ..
```

The `-b` flag adds as a regex pattern to block files you do not wish to
update in PyMongo. This is primarily helpful if you are implementing a
new feature in PyMongo that has spec tests already implemented, or if
you are attempting to validate new spec tests in PyMongo.

### Automated Specification Test Resyncing
The (`/.evergreen/scripts/resync-all-specs.sh`) script
automatically runs once a week to resync all the specs with the [specifications repo](https://github.com/mongodb/specifications).
A PR will be generated by mongodb-drivers-pr-bot containing any changes picked up by this resync.
The PR description will display the name(s) of the updated specs along
with any errors that occurred.

Spec test changes associated with a behavioral change or bugfix that has yet to be implemented in PyMongo
must be added to a patch file in `/.evergreen/spec-patch`. Each patch
file must be named after the associated PYTHON ticket and contain the
test differences between PyMongo's current tests and the specification.
All changes listed in these patch files will be *undone* by the script and won't
be applied to PyMongo's tests.

When a new test file or folder is added to the spec repo before the associated code changes are implemented, that test's path must be added to  `.evergreen/remove-unimplemented-tests.sh` along with a comment indicating the associated PYTHON ticket for those changes.

Any PR that implements a PYTHON ticket documented in a patch file or within  `.evergreen/remove-unimplemented-tests.sh` must also remove the associated patch file or entry in `remove-unimplemented-tests.sh`.

#### Adding to a patch file
To add to or create a patch file, run `git diff` to show the desired changes to undo and copy the
results into the patch file.

For example: the imaginary, unimplemented PYTHON-1234 ticket has associated spec test changes. To add those changes to `PYTHON-1234.patch`), do the following:
```bash
git diff HEAD~1 path/to/file >> .evergreen/spec-patch/PYTHON-1234.patch
```

#### Running Locally
Both `resync-all-specs.sh` and `resync-all-specs.py` can be run locally (and won't generate a PR).
```bash
./.evergreen/scripts/resync-all-specs.sh
python3 ./.evergreen/scripts/resync-all-specs.py
```

## Making a Release

Follow the [Python Driver Release Process Wiki](https://wiki.corp.mongodb.com/display/DRIVERS/Python+Driver+Release+Process).

## Asyncio considerations

PyMongo adds asyncio capability by modifying the source files in `*/asynchronous` to `*/synchronous` using
[unasync](https://github.com/python-trio/unasync/) and some custom transforms.

Where possible, edit the code in `*/asynchronous/*.py` and not the synchronous files.
You can run `pre-commit run --all-files synchro` before running tests if you are testing synchronous code.

To prevent the `synchro` hook from accidentally overwriting code, it first checks to see whether a sync version
of a file is changing and not its async counterpart, and will fail.
In the unlikely scenario that you want to override this behavior, first export `OVERRIDE_SYNCHRO_CHECK=1`.

Sometimes, the `synchro` hook will fail and introduce changes many previously unmodified files. This is due to static
Python errors, such as missing imports, incorrect syntax, or other fatal typos. To resolve these issues,
run `pre-commit run --all-files --hook-stage manual ruff` and fix all reported errors before running the `synchro`
hook again.

## Converting a test to async

The `tools/convert_test_to_async.py` script takes in an existing synchronous test file and outputs a
partially-converted asynchronous version of the same name to the `test/asynchronous` directory.
Use this generated file as a starting point for the completed conversion.

The script is used like so: `python tools/convert_test_to_async.py [test_file.py]`

## CPU profiling

To profile a test script and generate a flame graph, follow these steps:

1. Install `py-spy` if you haven't already:
   ```bash
   pip install py-spy
   ```
2. Inside your test script, perform any required setup and then loop over the code you want to profile for improved sampling.
3. Run `py-spy record -o <output.svg> -r <sample_rate=100> -- python <path/to/script>` to generate a `.svg` file containing the flame graph.
   (Note: on macOS you will need to run this command using `sudo` to allow `py-spy` to attach to the Python process.)
4. If you need to include native code (for example the C extensions), profiling should be done on a Linux system, as macOS and Windows do not support the `--native` option of `py-spy`.
   Creating an ubuntu Evergreen spawn host and using `scp` to copy the flamegraph `.svg` file back to your local machine is the best way to do this.
5. You can then view the flamegraph using an SVG viewer like a browser.

## Memory profiling

To test for a memory leak or any memory-related issues, the current best tool is [memray](https://bloomberg.github.io/memray/overview.html).
In order to include code from our C extensions, it must be run in native mode, on Linux.
To do so, either spin up an Ubuntu docker container or an Ubuntu Evergreen spawn host.

From the spawn host or Ubuntu image, do the following:

1. Install `memray` if you haven't already:
   ```bash
   pip install memray
   ```
2. Inside your test script, perform any required setup and then loop over the code you want to profile for improved sampling.
3. Run memray with the script under test with the `--native` flag, e.g. `python -m memray run --native -o test.bin <path/to/script>`.
4. Generate the flamegraph with `python -m memray flamegraph -o test.html test.bin`.
   See the [docs](https://bloomberg.github.io/memray/flamegraph.html) for more options.
5. Then, from the host computer, use either scp or docker cp to copy the flamegraph, e.g. `scp ubuntu@ec2-3-82-52-49.compute-1.amazonaws.com:/home/ubuntu/test.html .`.
6. You can then view the flamegraph html in a browser.

## Dependabot updates

Dependabot will raise PRs at most once per week, grouped by GitHub Actions updates and Python requirement
file updates.  We have a pre-commit hook that will update the `uv.lock` file when requirements change.
To update the lock file on a failing PR, you can use a method like `gh pr checkout <pr number>`, then run
`just lint uv-lock` to update the lock file, and then push the changes.  If a typing dependency has changed,
also run `just typing` and handle any new findings.
