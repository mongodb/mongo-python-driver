Installing / Upgrading
======================
.. highlight:: bash

**PyMongo** is in the `Python Package Index
<http://pypi.python.org/pypi/pymongo/>`_.

Microsoft Windows
-----------------

We recommend using the `MS Windows installers` available from the `Python
Package Index <http://pypi.python.org/pypi/pymongo/>`_.

Installing with pip
-------------------

We prefer `pip <http://pypi.python.org/pypi/pip>`_
to install pymongo on platforms other than Windows::

  $ pip install pymongo

To get a specific version of pymongo::

  $ pip install pymongo==1.10.1

To upgrade using pip::

  $ pip install --upgrade pymongo

Installing with easy_install
----------------------------

If you must install pymongo using
`setuptools <http://pypi.python.org/pypi/setuptools>`_ do::

  $ easy_install pymongo

To upgrade do::

  $ easy_install -U pymongo

Mac OS Snow Leopard Issues
--------------------------

By default OSX uses `/usr/bin/easy_install` for third party package installs.
In OSX 10.6.x this script is hardcoded to use a version of setuptools that is
older than the version required by PyMongo for python2.7 support. You can work
around it like this::

  $ easy_install -U setuptools
  $ python -m easy_install pymongo

To upgrade do::

  $ python -m easy_install -U pymongo

**Snow Leopard Xcode 4 Users**: The Python versions shipped with OSX 10.6.x
are universal binaries. They support i386, PPC, and (in the case of python2.6)
x86_64. Since Xcode 4 removed support for PPC the distutils version shipped
with Apple's builds of Python will fail to build the C extensions if you have
Xcode 4 installed. This issue may also affect some builds of Python downloaded
from python.org. There is a workaround::

  # For Apple-supplied Python2.6 (installed at /usr/bin/python2.6)
  $ env ARCHFLAGS='-arch i386 -arch x86_64' python -m easy_install pymongo

  # For 32-bit-only Python (/usr/bin/python2.5 and some builds
  # from python.org)
  $ env ARCHFLAGS='-arch i386' python -m easy_install pymongo

See `http://bugs.python.org/issue11623 <http://bugs.python.org/issue11623>`_
for a more detailed explanation.

Install from source
-------------------

If you'd rather install directly from the source (i.e. to stay on the
bleeding edge), check out the latest source from github and install
the driver from the resulting tree::

  $ git clone git://github.com/mongodb/mongo-python-driver.git pymongo
  $ cd pymongo/
  $ python setup.py install

Dependencies for installing C Extensions on Unix
------------------------------------------------

10gen does not currently provide statically linked binary packages for
Unix flavors other than OSX. To build the optional C extensions you must
have the GNU C compiler (gcc) installed. Depending on your flavor of Unix
(or Linux distribution) you may also need a python development package that
provides the necessary header files for your version of Python. The package
name may vary from distro to distro. Here are some examples for popular
Linux distributions:

- RHEL (Red Hat Enterprise Linux)/CentOS: python-devel
- Debian/Ubuntu: python-dev

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
