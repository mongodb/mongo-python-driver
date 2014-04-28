Some notes on PyMongo releases
==============================

Versioning
----------

We shoot for a release every few months - that will generally just
increment the middle version number (e.g. 2.6.3 -> 2.7).

Minor releases are reserved for bug fixes (in general no new features
or deprecations) - they only happen in cases where there is a critical
bug in a recently released version, or when a release has no new
features or API changes.

In between releases we add .devN to the version number to denote the version
under development. So if we just released 3.0, then the current dev
version would be 3.1.dev0. When we make the next release we
replace all instances of 3.1.devN in the docs with the new version number.

Deprecation
-----------

Changes should be backwards compatible unless absolutely necessary. When making
API changes the approach is generally to add a deprecation warning but keeping
the existing API functional. Eventually (after at least ~4 releases) we can
remove the old API.

Doing a Release
---------------

1. Test release on Python 2.6-2.7 and 3.2-3.4 on Windows, Linux and OSX,
   with and without the C extension. Generally enough to just run the tests on
   2.6, 2.7, 3.2 and the latest 3.x version with and without the extension on
   a single platform, and then just test any version on the other platforms
   as a sanity check. `python setup.py test` will build the extension
   and test.  `python tools/clean.py` will remove the extension, and
   then `python setup.py --no_ext test` will run the tests without
   it. Run the replica set and mongos high-availability tests with
   `PYTHONPATH=. python test/high_availability/test_ha.py`. Can also
   run the doctests: `python setup.py doc -t`. For building extensions
   on Windows check section below.

2. Add release notes to doc/changelog.rst. Generally just summarize/clarify
   the git log, but might add some more long form notes for big changes.

3. Search and replace the "devN" version number w/ the new version number (see
   note above).

4. Make sure version number is updated in setup.py and pymongo/__init__.py

5. Commit with a BUMP version_number message.

6. Tag w/ version_number

7. Push commit / tag.

8. Push source to PyPI: `python setup.py sdist upload`

9. Push binaries to PyPI; for each version of python and platform do:`python
   setup.py bdist_egg upload`. Probably best to do `python setup.py bdist_egg`
   first, to make sure the egg builds properly. On Windows we also push a binary
   installer. The setup.py target for that is `bdist_wininst`.

10. Make sure to push a build of the new docs (see the apidocs repo).

11. Bump the version number to <next version>.dev0 in setup.py/__init__.py,
    commit, push.

12. Announce!
