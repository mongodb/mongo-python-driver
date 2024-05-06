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

PyMongo supports CPython 3.8+ and PyPy3.9+. Language features not
supported by all interpreters can not be used.

## Style Guide

PyMongo follows [PEP8](http://www.python.org/dev/peps/pep-0008/)
including 4 space indents and 79 character line limits.

## General Guidelines

-   Avoid backward breaking changes if at all possible.
-   Write inline documentation for new classes and methods.
-   Write tests and make sure they pass (make sure you have a mongod
    running on the default port, then execute `tox -e test` from the cmd
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

To run a manual hook like `mypy` manually, run:

```bash
pre-commit run --all-files --hook-stage manual mypy
```

Typically we use `tox` to run the linters, e.g.

```bash
tox -e typecheck-mypy
tox -e lint-manual
```

## Documentation

To contribute to the [API
documentation](https://pymongo.readthedocs.io/en/stable/) just make your
changes to the inline documentation of the appropriate [source
code](https://github.com/mongodb/mongo-python-driver) or [rst
file](https://github.com/mongodb/mongo-python-driver/tree/master/doc) in
a branch and submit a [pull
request](https://help.github.com/articles/using-pull-requests). You
might also use the GitHub
[Edit](https://github.com/blog/844-forking-with-the-edit-button) button.

We use [reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html) for all
documentation including narrative docs, and the [Sphinx docstring format](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html).

You can build the documentation locally by running:

```bash
tox -e doc
```

When updating docs, it can be helpful to run the live docs server as:

```bash
tox -e doc-serve
```

Browse to the link provided, and then as you make changes to docstrings or narrative docs,
the pages will re-render and the browser will automatically refresh.


## Running Tests Locally

-   Ensure you have started the appropriate Mongo Server(s).
-   Run `pip install tox` to use `tox` for testing or run
    `pip install -e ".[test]"` to run `pytest` directly.
-   Run `tox -m test` or `pytest` to run all of the tests.
-   Append `test/<mod_name>.py::<class_name>::<test_name>` to run
    specific tests. You can omit the `<test_name>` to test a full class
    and the `<class_name>` to test a full module. For example:
    `tox -m test -- test/test_change_stream.py::TestUnifiedChangeStreamsErrors::test_change_stream_errors_on_ElectionInProgress`.
-   Use the `-k` argument to select tests by pattern.

## Running Load Balancer Tests Locally

-   Install `haproxy` (available as `brew install haproxy` on macOS).
-   Clone `drivers-evergreen-tools`:
    `git clone git@github.com:mongodb-labs/drivers-evergreen-tools.git`.
-   Start the servers using
    `LOAD_BALANCER=true TOPOLOGY=sharded_cluster AUTH=noauth SSL=nossl MONGODB_VERSION=6.0 DRIVERS_TOOLS=$PWD/drivers-evergreen-tools MONGO_ORCHESTRATION_HOME=$PWD/drivers-evergreen-tools/.evergreen/orchestration $PWD/drivers-evergreen-tools/.evergreen/run-orchestration.sh`.
-   Start the load balancer using:
    `MONGODB_URI='mongodb://localhost:27017,localhost:27018/' $PWD/drivers-evergreen-tools/.evergreen/run-load-balancer.sh start`.
-   Run the tests from the `pymongo` checkout directory using:
    `TEST_LOADBALANCER=1 tox -m test-eg`.

## Running Encryption Tests Locally
- Run `AWS_PROFILE=<profile> tox -m setup-encryption` after setting up your AWS profile with `aws configure sso`.
- Run the tests with `TEST_ENCRYPTION=1 tox -e test-eg`.
- When done, run `tox -m teardown-encryption` to clean up.

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
