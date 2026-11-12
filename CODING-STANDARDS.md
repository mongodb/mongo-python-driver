# Intent

This document describes the common coding standards for all projects maintained by the Python Drivers team. These standards apply to all written code, comments, and docstrings, both human and machine-authored. Use these guidelines both when writing and reviewing code.

This is a living document: when an issue or difference of opinion occurs several times across reviews or designs, the final resolution should be added as a new guiding rule here.

# Comments

1. Comments provide additional context for code when that context is not apparent in the code itself. Code that is self-explanatory should never have a comment.
* A good comment:

```py
# SSLSocket can have buffered data which won't be caught by select.
if hasattr(sock, "pending") and sock.pending() > 0:
    readable = True
```

* An unnecessary comment:

```py
# Check if the socket is pending.
if hasattr(sock, "pending") and sock.pending() > 0:
    readable = True
```

2. Comments are concise.
3. Comments use proper English grammar: start with a capital letter, use correct punctuation, and end with a period.
4. Avoid metaphors, similes, analogies, and other figurative language.

# Docstrings

1. Follow [PEP 257](https://peps.python.org/pep-0257/)’s conventions alongside the following example Sphinx formatting (this is for [AsyncCollection.distinct](https://github.com/mongodb/mongo-python-driver/blob/7512bad908e24a248a36cd286fa7c9ab1663da3d/pymongo/asynchronous/collection.py#L3147)()):

```py
"""Get a list of distinct values for `key` among all documents
in this collection.

Raises :class:`TypeError` if `key` is not an instance of
:class:`str`.

All optional distinct parameters should be passed as keyword arguments
to this method. Valid options include:

  - `maxTimeMS` (int): The maximum amount of time to allow the count
    command to run, in milliseconds.
  - `collation` (optional): An instance of
    :class:`~pymongo.collation.Collation`.

The :meth:`distinct` method obeys the :attr:`read_preference` of
this :class:`AsyncCollection`.

:param key: Name of the field for which we want to get the distinct
    values.
:param filter: A query document that specifies the documents
    from which to retrieve the distinct values.
:param session: A
    :class:`~pymongo.asynchronous.client_session.AsyncClientSession`.
:param comment: A user-provided comment to attach to this
    command.
:param hint: An index to use to support the query
    predicate specified either by its string name, or in the same
    format as passed to :meth:`~pymongo.asynchronous.collection.AsyncCollection.create_index`
    (e.g. ``[('field', ASCENDING)]``).
:param kwargs: See list of options above.

.. versionchanged:: 4.12
   Added ``hint`` parameter.

.. versionchanged:: 3.6
   Added ``session`` parameter.

.. versionchanged:: 3.4
   Support the `collation` option.

"""
```

2. Docstrings tell the reader the purpose and behavior of an API. They do not describe internal implementation details.
3. Include what errors can be raised from the API.

# Code Style

1. Follow [PEP 8](https://peps.python.org/pep-0008/)’s conventions, mostly enforced by ruff and pre-commit. Note that we use a line length of 100 characters rather than 79\.
2. Avoid [magic numbers](https://en.wikipedia.org/wiki/Magic_number_\(programming\)).

# Code Structure and Design

1. Prefer shared helpers and abstraction over code duplication.
2. Bias towards less code.
3. Use consistent patterns for similar code.
4. Consider race conditions.
5. Handle or explicitly rule out edge cases with a comment if the reason is non-obvious or undocumented.
6. Prioritize future maintainability. If you would be unhappy maintaining a piece of code five years in the future, do not commit it.
7. When updating a method or class with multiple arguments, append the argument name unless strictly necessary.

# Error Handling

1. Raise or catch specific rather than general error types:

```py
# Do this
except OperationFailure:
...

# Not this
except PyMongoError:
...

# Not this
except OperationFailure:
raise Exception(...)

```

2. Use “raise … from …” syntax when chaining errors.

# Performance

1. Avoid doing unnecessary work: if an operation can be avoided in certain conditions, check those conditions before performing it.
* Do this:

```py
if value == "abc":
    return
result = expensive_function_call(value)
if result == ...
```

* Avoid this:

```py
result = expensive_function_call(value)
if value == "abc":
    return
if result == ...
```

2. Hold locks only for the minimal critical section.
3. Run the [performance benchmarking suite](https://github.com/mongodb/mongo-python-driver/blob/main/CONTRIBUTING.md#perf-tests) to catch possible regressions for large feature or performance changes.

# Testing

1. All tests should pass; if one does not pass and you can prove that it is unrelated to your current changes, note the failure in your PR before merging.
2. All code reachable through a public API must have test coverage.
3. Prefer many small tests over one large one.
4. Use [unittest-style assertions](https://docs.python.org/3/library/unittest.html#basic-example) when using unittest.
5. Setup and cleanup test resources safely for both success and failure.
6. Prefer unit tests instead of integration tests where possible.

# Typing

1. Annotate public APIs and verify using “mypy \--strict”.
2. Type-only dependencies should be imported within a TYPE\_CHECKING block:

```py
from pymongo import MongoClient

if TYPE_CHECKING:
	from pymongo.collection import Collection
```

# Backwards compatibility

1. We support all of the currently maintained CPython versions. All code must be compatible with the oldest supported version, which is 3.11 as of October 2026\.
2. Avoid breaking changes when possible.
3. Follow [semantic versioning](https://semver.org/) guidelines.
4. When deprecating a public API, add a deprecation warning to every call, a “deprecated::” line in its docstring, and a note in the changelog of the deprecating release.
   1. Changes marked `deprecated::` get removed in the next **major** release

# Changelog

1. All user-facing changes require a changelog entry.
2. Changelog entries are concise summaries of their described changes written using proper English grammar and in the past tense.
3. Changelog entries must start with one of the following words that categorize the type of change:
   1. `Added`: new features.
   2. `Changed`: changes in existing functionality.
   3. `Deprecated`: features to be removed in a future version.
   4. `Removed`: features removed in the described version.
   5. `Fixed`: bug fixes.
   6. `Security`: patched vulnerabilities.
4. Group all entries of the same type together in each version.
5. Changelog entries do not contain implementation details.
6. Use the guiding principle of "what would a user want to know about this" when describing a change.
7. Changelog entries must end with a link to the JIRA ticket that implemented their changes.
8. Cross-references to code must use one of the following Sphinx roles:
   1. `:meth:`: methods.
   2. `:class:`: classes.
   3. `:attr:`: attributes and properties.
   4. `:mod:`: modules.
   5. `:exc:`: exceptions.
   6. `:func:`: functions.
   7. `:data:`: module-level variables.
   8. `:const:`: constants.
9. Code snippets that do not fit any of the above roles must be contained within double backticks: ``text``.

# Git

1. Commit messages are concise summaries of their described changes.
2. Commit messages use proper English grammar except they do not end with a period.
3. Commit messages are written in the [imperative mood](https://en.wikipedia.org/wiki/Imperative_mood): they complete the sentence “this commit will …”.
   1. A good commit message: “Improve insert performance for large documents”
   2. A poor commit message: “2x speedup on inserts (if document \> 1MB).”
4. Extended messages longer than the one line summary are permitted, but are not encouraged as they are squashed during pull-request merge. They may contain periods.
5. Do not force-push to an open PR unless absolutely necessary.
