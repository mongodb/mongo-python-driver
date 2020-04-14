Contributing to PyMongo
=======================

PyMongo has a large `community
<https://pymongo.readthedocs.io/en/stable/contributors.html>`_ and
contributions are always encouraged. Contributions can be as simple as
minor tweaks to the documentation. Please read these guidelines before
sending a pull request.

Bugfixes and New Features
-------------------------

Before starting to write code, look for existing `tickets
<https://jira.mongodb.org/browse/PYTHON>`_ or `create one
<https://jira.mongodb.org/browse/PYTHON>`_ for your specific
issue or feature request. That way you avoid working on something
that might not be of interest or that has already been addressed.

Supported Interpreters
----------------------

PyMongo supports CPython 2.7, 3.4+, PyPy, and PyPy3.5+. Language
features not supported by all interpreters can not be used.

Style Guide
-----------

PyMongo follows `PEP8 <http://www.python.org/dev/peps/pep-0008/>`_
including 4 space indents and 79 character line limits.

General Guidelines
------------------

- Avoid backward breaking changes if at all possible.
- Write inline documentation for new classes and methods.
- Write tests and make sure they pass (make sure you have a mongod
  running on the default port, then execute ``python setup.py test``
  from the cmd line to run the test suite).
- Add yourself to doc/contributors.rst :)

Documentation
-------------

To contribute to the `API documentation <https://pymongo.readthedocs.io/en/stable/>`_
just make your changes to the inline documentation of the appropriate
`source code <https://github.com/mongodb/mongo-python-driver>`_ or `rst file
<https://github.com/mongodb/mongo-python-driver/tree/master/doc>`_ in a
branch and submit a `pull request <https://help.github.com/articles/using-pull-requests>`_.
You might also use the GitHub `Edit <https://github.com/blog/844-forking-with-the-edit-button>`_
button.
