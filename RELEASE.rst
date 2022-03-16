Some notes on PyMongo releases
==============================

Versioning
----------

We shoot for a release every few months - that will generally just
increment the middle / minor version number (e.g. 3.5.0 -> 3.6.0).

Patch releases are reserved for bug fixes (in general no new features
or deprecations) - they only happen in cases where there is a critical
bug in a recently released version, or when a release has no new
features or API changes.

In between releases we add .devN to the version number to denote the version
under development. So if we just released 3.6.0, then the current dev
version might be 3.6.1.dev0 or 3.7.0.dev0. When we make the next release we
replace all instances of 3.x.x.devN in the docs with the new version number.

https://semver.org/
https://www.python.org/dev/peps/pep-0440/

Deprecation
-----------

Changes should be backwards compatible unless absolutely necessary. When making
API changes the approach is generally to add a deprecation warning but keeping
the existing API functional. Deprecated features can be removed in a release
that changes the major version number.

Doing a Release
---------------

1. PyMongo is tested on Evergreen. Ensure the latest commit are passing CI
   as expected: https://evergreen.mongodb.com/waterfall/mongo-python-driver.
   To test locally, ``python3 setup.py test`` will build the C extensions and
   test. ``python3 tools/clean.py`` will remove the extensions,
   and then ``python3 setup.py --no_ext test`` will run the tests without
   them. You can also run the doctests: ``python3 setup.py doc -t``.

2. Check Jira to ensure all the tickets in this version have been completed.

3. Add release notes to doc/changelog.rst. Generally just summarize/clarify
   the git log, but you might add some more long form notes for big changes.

4. Search and replace the "devN" version number w/ the new version number (see
   note above in `Versioning`_).

5. Make sure version number is updated in setup.py and pymongo/__init__.py

6. Commit with a BUMP version_number message, eg ``git commit -m 'BUMP 3.11.0'``.

7. Tag w/ version_number, eg, ``git tag -a '3.11.0' -m '3.11.0' <COMMIT>``.

8. Push commit / tag, eg ``git push && git push --tags``.

9. Pushing a tag will trigger a release process in Evergreen which builds
   wheels for manylinux, macOS, and Windows. Wait for the "release-combine"
   task to complete and then download the "Release files all" archive. See:
   https://evergreen.mongodb.com/waterfall/mongo-python-driver?bv_filter=release

   The contents should look like this::

     $ ls path/to/archive
     pymongo-<version>-cp310-cp310-macosx_10_9_universal2.whl
     ...
     pymongo-<version>-cp38-cp38-manylinux2014_x86_64.whl
     ...
     pymongo-<version>-cp38-cp38-win_amd64.whl
     ...
     pymongo-<version>.tar.gz

10. Upload all the release packages to PyPI with twine::

     $ python3 -m twine upload path/to/archive/*

11. Make sure the new version appears on https://pymongo.readthedocs.io/. If the
    new version does not show up automatically, trigger a rebuild of "latest":
    https://readthedocs.org/projects/pymongo/builds/

12. Bump the version number to <next version>.dev0 in setup.py/__init__.py,
    commit, push.

13. Publish the release version in Jira.

14. Announce the release on:
    https://developer.mongodb.com/community/forums/c/community/release-notes/

15. File a ticket for DOCSP highlighting changes in server version and Python
    version compatibility or the lack thereof, for example:
    https://jira.mongodb.org/browse/DOCSP-13536

16. Create a GitHub Release for the tag using
    https://github.com/mongodb/mongo-python-driver/releases/new.
    The title should be "PyMongo X.Y.Z", and the description should contain
    a link to the release notes on the the community forum, e.g.
    "Release notes: mongodb.com/community/forums/t/pymongo-4-0-2-released/150457."
