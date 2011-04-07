Installing / Upgrading
======================
.. highlight:: bash

**PyMongo** is in the `Python Package Index
<http://pypi.python.org/pypi/pymongo/>`_. To install PyMongo using
`setuptools <http://pypi.python.org/pypi/setuptools>`_ do::

  $ easy_install pymongo

To upgrade do::

  $ easy_install -U pymongo

If you'd rather install directly from the source (i.e. to stay on the
bleeding edge), check out the latest source from github and install
the driver from the resulting tree::

  $ git clone git://github.com/mongodb/mongo-python-driver.git pymongo
  $ cd pymongo/
  $ python setup.py install

Dependencies for installing C Extensions on Unix
------------------------------------------------

10gen does not currently provide statically linked binary packages for
Unix. To build the optional C extensions you must have the GNU C compiler
(gcc) installed. Depending on your flavor of Unix (or Linux distribution)
you may also need a python-dev package that provides the necessary header
files for your version of Python. The package name may vary from distro
to distro.

Pre-built eggs are available for OSX Snow Leopard on PyPI.

.. _install-no-c:

Installing Without C Extensions
-------------------------------
By default, the driver attempts to build and install optional C
extensions (used for increasing performance) when it is installed. If
any extension fails to build the driver will be installed anyway but a
warning will be printed.

In :ref:`certain cases <using-with-mod-wsgi>`, you might wish to
install the driver without the C extensions, even if the extensions
build properly. This can be done using a command line option to
*setup.py*::

  $ python setup.py --no_ext install
