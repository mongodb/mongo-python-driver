Some notes on PyMongo releases
==============================

Versioning
----------

We shoot for a release every month or so - that will generally just
increment the middle version number (e.g. 1.6.1 -> 1.7). We're
getting to the point where a 2.0 release would be reasonable, though -
a lot has changed since 1.0.

Minor releases are reserved for bug fixes (in general no new features
or deprecations) - they only happen in cases where there is a critical
bug in a recently released version, or when a release has no new
features or API changes.

In between releases we use a "+" version number to denote the version
under development. So if we just released 1.6, then the current dev
version would be 1.6+. When we make the next release (1.6.1 or 1.7) we
replace all instances of 1.6+ in the docs with the new version number.

Deprecation
-----------

Changes should be backwards compatible unless absolutely
necessary. When making API changes the approach is generally to add a
deprecation warning but keeping the existing API
functional. Eventually (after at least ~4 releases) we can remove the
old API.

Doing a Release
---------------

1. Test release on Python 2.4-2.7 on Windows, Linux and OSX, with and without the C extension. Generally enough to just run the tests on 2.4 and 2.7 with and without the extension on a single platform, and then just test any version on the other platforms as a sanity check. `python setup.py test` will build the extension and test. `python tools/clean.py` will remove the extension, and then `nosetests` will run the tests without it. Can also run the doctests: `python setup.py doc -t`. For building extensions on Windows check section below.

2. Add release notes to doc/changelog.rst. Generally just summarize/clarify the git log, but might add some more long form notes for big changes.

3. Search and replace the "+" version number w/ the new version number (see note above).

4. Make sure version number is updated in setup.py and pymongo/__init__.py

5. Commit with a BUMP version_number message.

6. Tag w/ version_number

7. Push commit / tag.

8. Push source to PyPI: `python setup.py sdist upload`

9. Push binaries to PyPI; for each version of python and platform do: `python setup.py bdist_egg upload`. Probably best to do `python setup.py bdist_egg` first, to make sure the egg builds properly. Notably on the Windows machine, for Python 2.4 and 2.5, you will have to run `python setup.py build -c mingw32 bdist_egg upload` or the C extension build will fail with an error about Visual Studio 2003. On Windows we also push a binary installer. The setup.py target for that is `bdist_wininst`.

10. Make sure the docs have properly updated (driver buildbot does this).

11. Add a "+" to the version number in setup.py/__init__.py, commit, push.

12. Announce!

Building extensions on Windows
------------------------------
Currently the default "python setup.py tests" only builds extensions on Windows 32 bit only. Also the default option expects Visual Studio 2008. This has been tested with python 2.6 and 2.7

1. On your Windows 32 bit machine install Microsoft Visual C++ 2008 Express Edition (or equivalent 2008 edition) in the default location

2. Ensure you have nose installed

3. Run python setup.py tests to build the C extensions and run pymongo tests.