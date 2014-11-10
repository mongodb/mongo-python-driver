Compatibility Policy
====================

Semantic Versioning
-------------------

PyMongo's version numbers follow `semantic versioning`_: each version number
is structured "major.minor.patch". Patch releases fix bugs, minor releases
add features (and may fix bugs), and major releases include API changes that
break backwards compatibility (and may add features and fix bugs).

Deprecation
-----------

Before we remove a feature in a major release, PyMongo's maintainers make an
effort to release at least one minor version that *deprecates* it. We add
"**DEPRECATED**" to the feature's documentation, and update the code to raise a
`DeprecationWarning`_. You can ensure your code is future-proof by running
your code with the latest PyMongo release and looking for DeprecationWarnings.

Starting with Python 2.7, the interpreter silences DeprecationWarnings by
default. For example, the following code uses the deprecated ``slave_okay``
option but does not raise any warning:

.. code-block:: python

    # "slave_okay.py"
    from pymongo import MongoClient

    client = MongoClient(slave_okay=True)

To print deprecation warnings to stderr, run python with "-Wd"::

  $ python -Wd slave_okay.py
  slave_okay.py:4: DeprecationWarning: slave_okay is deprecated. Please use read_preference instead.
    client = MongoClient(slave_okay=True)

You can turn warnings into exceptions with "python -We"::

  $ python -We slave_okay.py
  Traceback (most recent call last):
    File "slave_okay.py", line 4, in <module>
      client = MongoClient(slave_okay=True)
    File "/Users/emptysquare/.virtualenvs/official/mongo-python-driver/pymongo/mongo_client.py", line 373, in __init__
      stacklevel=2)
  DeprecationWarning: slave_okay is deprecated. Please use read_preference instead.

If your own code's test suite passes with "python -We" then it uses no
deprecated PyMongo features.

.. seealso:: The Python documentation on `the warnings module`_,
   and `the -W command line option`_.

.. _semantic versioning: http://semver.org/

.. _DeprecationWarning:
  https://docs.python.org/2/library/exceptions.html#exceptions.DeprecationWarning

.. _the warnings module: https://docs.python.org/2/library/warnings.html

.. _the -W command line option: https://docs.python.org/2/using/cmdline.html#cmdoption-W
