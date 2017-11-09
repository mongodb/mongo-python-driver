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
default. For example, the following code uses the deprecated ``insert``
method but does not raise any warning:

.. code-block:: python

    # "insert.py"
    from pymongo import MongoClient

    client = MongoClient()
    client.test.test.insert({})

To print deprecation warnings to stderr, run python with "-Wd"::

  $ python -Wd insert.py
  insert.py:4: DeprecationWarning: insert is deprecated. Use insert_one or insert_many instead.
    client.test.test.insert({})

You can turn warnings into exceptions with "python -We"::

  $ python -We insert.py
  Traceback (most recent call last):
    File "insert.py", line 4, in <module>
      client.test.test.insert({})
    File "/home/durin/work/mongo-python-driver/pymongo/collection.py", line 2906, in insert
      "instead.", DeprecationWarning, stacklevel=2)
  DeprecationWarning: insert is deprecated. Use insert_one or insert_many instead.

If your own code's test suite passes with "python -We" then it uses no
deprecated PyMongo features.

.. seealso:: The Python documentation on `the warnings module`_,
   and `the -W command line option`_.

.. _semantic versioning: http://semver.org/

.. _DeprecationWarning:
  https://docs.python.org/2/library/exceptions.html#exceptions.DeprecationWarning

.. _the warnings module: https://docs.python.org/2/library/warnings.html

.. _the -W command line option: https://docs.python.org/2/using/cmdline.html#cmdoption-W
