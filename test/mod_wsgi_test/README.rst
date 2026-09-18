Testing PyMongo with mod_wsgi
=============================

These tests verify that PyMongo works with Apache and mod_wsgi. They are
primarily intended to prevent regression of
`PYTHON-353 <https://jira.mongodb.org/browse/PYTHON-353>`_, a connection leak
when PyMongo 2.2 was used with Python 2.6 and mod_wsgi 2.8. However, the test
may also catch concurrency bugs, or incompatibilities between PyMongo's C
extensions and the way mod_wsgi manages Python sub interpreters. It is
generally useful to test PyMongo in the unconventional environment that
mod_wsgi creates.

Test Matrix
-----------

Continuous integration tests the oldest supported CPython against the oldest
supported MongoDB with minimum dependencies, and the newest supported CPython
against the latest MongoDB, in both daemon and embedded mode against a replica
set. The Python and MongoDB versions come from ``generate_config_utils.py`` in
``.evergreen/scripts``. Other combinations can be tested manually.

Setup
-----

Install Apache
..............

On Ubuntu, install Apache and the headers used to build mod_wsgi::

    sudo apt-get install -y apache2 apache2-dev

Install mod_wsgi
................

The project defines a ``mod_wsgi`` dependency group used for testing. pip
builds mod_wsgi against the interpreter it is installed with::

    uv sync --group mod_wsgi

Start mongod
............

Start a standalone listening on port 27017, or a replica set with a member
listening on port 27017.

Configure Apache
................

Set a MOD_WSGI_SO environment variable so our ``mod_wsgi_test.conf``
can find and load mod_wsgi.so::

    export MOD_WSGI_SO=$(find .venv -name "mod_wsgi*.so")

Start Apache with one of the config files in this directory.

Run the test
------------

Run the included ``test_client.py`` script::

    python test/mod_wsgi_test/test_client.py -n 2500 -t 100 parallel \
         http://localhost/interpreter1${WORKSPACE} http://localhost/interpreter2${WORKSPACE}

...where the "n" argument is the total number of requests to make to Apache,
and "t" specifies the number of threads. ``WORKSPACE`` is the location of
the PyMongo checkout. Note that multiple URLs are passed, each one corresponds
to a different sub interpreter.

Run this script again with different arguments to make serial requests::

    python test/mod_wsgi_test/test_client.py -n 25000 serial \
        http://localhost/interpreter1${WORKSPACE} http://localhost/interpreter2${WORKSPACE}

The ``test_client.py`` script merely makes HTTP requests to Apache. Its
exit code is non-zero if any of its requests fails, for example with an
HTTP 500.

The core of the test is in the WSGI script, ``mod_wsgi_test.py``.
This script inserts some documents into MongoDB at startup, then queries
documents for each HTTP request.

If PyMongo is leaking connections and "n" is much greater than the ulimit,
the test will fail when PyMongo exhausts its file descriptors.

The script also encodes and decodes all BSON types to ensure that
multiple sub interpreters in the same process are supported. This tests
the workaround added in `PYTHON-569 <https://jira.mongodb.org/browse/PYTHON-569>`_.

Automation
----------

Continuous integration runs the test on every pull request in the Mod WSGI
job of `.github/workflows/test-python.yml
<https://github.com/mongodb/mongo-python-driver/blob/master/.github/workflows/test-python.yml>`_.
To run the same steps locally, use ``just smoke-mod-wsgi``, which runs an
ubuntu container with Apache, a single-node replica set, and both test modes.
