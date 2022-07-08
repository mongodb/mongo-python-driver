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

PyMongo supports CPython 3.7+ and PyPy3.7+. Language
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

Running Linters
---------------

PyMongo uses `pre-commit <https://pypi.org/project/pre-commit/>`_
for managing linting of the codebase.
``pre-commit`` performs various checks on all files in PyMongo and uses tools
that help follow a consistent code style within the codebase.

To set up ``pre-commit`` locally, run::

    pip install pre-commit
    pre-commit install

To run ``pre-commit`` manually, run::

    pre-commit run --all-files

To run a manual hook like `flake8` manually, run::

    pre-commit run --all-files --hook-stage manual flake8

Documentation
-------------

To contribute to the `API documentation <https://pymongo.readthedocs.io/en/stable/>`_
just make your changes to the inline documentation of the appropriate
`source code <https://github.com/mongodb/mongo-python-driver>`_ or `rst file
<https://github.com/mongodb/mongo-python-driver/tree/master/doc>`_ in a
branch and submit a `pull request <https://help.github.com/articles/using-pull-requests>`_.
You might also use the GitHub `Edit <https://github.com/blog/844-forking-with-the-edit-button>`_
button.

Running Tests Locally
---------------------
- Ensure you have started the appropriate Mongo Server(s)
- Run ``python setup.py test`` to run all of the tests
- Run ``python setup.py test -s test.<mod_name>.<class_name>.<test_name>`` to
run specific tests.  You can omit the ``<test_name>`` to test a full class
and the ``<class_name>`` to test a full module.  For example:
``python setup.py test -s test.test_change_stream.TestUnifiedChangeStreamsErrors.test_change_stream_errors_on_ElectionInProgress``

Running Load Balancer Tests Locally
-----------------------------------
- Install ``haproxy`` (available as ``brew install haproxy`` on macOS).
- Clone ``drivers-evergreen-tools``: ``git clone git@github.com:mongodb-labs/drivers-evergreen-tools.git``
- Start the servers using ``LOAD_BALANCER=true TOPOLOGY=sharded_cluster AUTH=noauth SSL=nossl MONGODB_VERSION=6.0 DRIVERS_TOOLS=./drivers-evergreen-tools MONGO_ORCHESTRATION_HOME=./drivers-evergreen-tools/.evergreen/orchestration ./drivers-evergreen-tools/.evergreen/run-orchestration.sh``
- Start the load balancer using: ``MONGODB_URI='mongodb://localhost:27017,localhost:27018/' .evergreen/run-load-balancer.sh start``.
- Run the tests using: ``LOADBALANCER=1 TEST_LOADBALANCER=1 SINGLE_MONGOS_LB_URI='mongodb://127.0.0.1:8000/?loadBalanced=true' MULTI_MONGOS_LB_URI='mongodb://127.0.0.1:8001/?loadBalanced=true' MONGODB_URI='mongodb://localhost:27017,localhost:27018/' python setup.py test -s test.test_load_balancer``

Re-sync Spec Tests
------------------

If you would like to re-sync the copy of the specification tests in the
PyMongo repository with that which is inside the `specifications repo
<https://github.com/mongodb/specifications>`_, please
use the script provided in ``.evergreen/resync-specs.sh``.::

    git clone git@github.com:mongodb/specifications.git
    export MDB_SPECS=~/specifications
    cd ~/mongo-python-driver/.evergreen
    ./resync-specs.sh -b "connection-string*" crud bson-corpus
    cd ..

The ``-b`` flag adds as a regex pattern to block files you do not wish to
update in PyMongo.
This is primarily helpful if you are implementing a new feature in PyMongo
that has spec tests already implemented, or if you are attempting to
validate new spec tests in PyMongo.
