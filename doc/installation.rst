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
