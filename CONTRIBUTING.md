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

PyMongo supports CPython 3.9+ and PyPy3.10+. Language features not
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

-   Ensure you have started the appropriate Mongo Server(s).
-   Run `just install` to set a local virtual environment, or you can manually
    create a virtual environment and run `pytest` directly.  If you want to use a specific
    version of Python, remove the `.venv` folder and set `PYTHON_BINARY` before running `just install`.
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
- Set up access to [Drivers test secrets](https://github.com/mongodb-labs/drivers-evergreen-tools/tree/master/.evergreen/secrets_handling#secrets-handling).

### Usage

- Run `just run-server` with optional args to set up the server.
  All given flags will be passed to `run-orchestration.sh` in `$DRIVERS_TOOLS`.
- Run `just setup-tests` with optional args to set up the test environment, secrets, etc.
- Run `just run-tests` to run the tests in an appropriate Python environment.
- When done, run `just teardown-tests` to clean up and `just stop-server` to stop the server.

### Encryption tests

- Run `just run-server` to start the server.
- Run `just setup-tests encryption`.
- Run the tests with `just run-tests`.

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

### Atlas Data Lake tests.

You must have `docker` or `podman` installed locally.

- Run `just setup-tests data_lake`.
- Run `just run-tests`.

### OCSP tests

- Export the orchestration file, e.g. `export ORCHESTRATION_FILE=rsa-basic-tls-ocsp-disableStapling.json`.
This corresponds to a config file in `$DRIVERS_TOOLS/.evergreen/orchestration/configs/servers`.
MongoDB servers on MacOS and Windows do not staple OCSP responses and only support RSA.
- Run `just run-server ocsp`.
- Run `just setup-tests ocsp <sub test>` (options are "valid", "revoked", "valid-delegate", "revoked-delegate").
- Run `just run-tests`

If you are running one of the `no-responder` tests, omit the `run-server` step.

### Perf Tests

- Start the appropriate server, e.g. `just run-server --version=v8.0-perf --ssl`.
- Set up the tests with `sync` or `async`: `just setup-tests perf sync`.
- Run the tests: `just run-tests`.

## Enable Debug Logs
- Use `-o log_cli_level="DEBUG" -o log_cli=1` with `just test` or `pytest`.
- Add `log_cli_level = "DEBUG` and `log_cli = 1` to the `tool.pytest.ini_options` section in `pyproject.toml` for Evergreen patches or to enable debug logs by default on your machine.
- You can also set `DEBUG_LOG=1` and run either `just setup-tests` or `just-test`.
- For evergreen patch builds, you can use `evergreen patch --param DEBUG_LOG=1` to enable debug logs for the patch.

## Re-sync Spec Tests

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
