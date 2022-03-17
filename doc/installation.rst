Installing / Upgrading
======================
.. highlight:: bash

**PyMongo** is in the `Python Package Index
<http://pypi.python.org/pypi/pymongo/>`_.

.. warning:: **Do not install the "bson" package from pypi.** PyMongo comes
   with its own bson package; doing "pip install bson" or "easy_install bson"
   installs a third-party package that is incompatible with PyMongo.

Installing with pip
-------------------

We recommend using `pip <http://pypi.python.org/pypi/pip>`_
to install pymongo on all platforms::

  $ python3 -m pip install pymongo

To get a specific version of pymongo::

  $ python3 -m pip install pymongo==3.5.1

To upgrade using pip::

  $ python3 -m pip install --upgrade pymongo

Dependencies
------------

PyMongo supports CPython 3.6.2+ and PyPy3.6+.

Optional dependencies:

GSSAPI authentication requires `pykerberos
<https://pypi.python.org/pypi/pykerberos>`_ on Unix or `WinKerberos
<https://pypi.python.org/pypi/winkerberos>`_ on Windows. The correct
dependency can be installed automatically along with PyMongo::

  $ python3 -m pip install "pymongo[gssapi]"

:ref:`MONGODB-AWS` authentication requires `pymongo-auth-aws
<https://pypi.org/project/pymongo-auth-aws/>`_::

  $ python3 -m pip install "pymongo[aws]"

Support for mongodb+srv:// URIs requires `dnspython
<https://pypi.python.org/pypi/dnspython>`_::

  $ python3 -m pip install "pymongo[srv]"

:ref:`OCSP` requires `PyOpenSSL
<https://pypi.org/project/pyOpenSSL/>`_, `requests
<https://pypi.org/project/requests/>`_ and `service_identity
<https://pypi.org/project/service_identity/>`_::

  $ python3 -m pip install "pymongo[ocsp]"

Wire protocol compression with snappy requires `python-snappy
<https://pypi.org/project/python-snappy>`_::

  $ python3 -m pip install "pymongo[snappy]"

Wire protocol compression with zstandard requires `zstandard
<https://pypi.org/project/zstandard>`_::

  $ python3 -m pip install "pymongo[zstd]"

:ref:`Client-Side Field Level Encryption` requires `pymongocrypt
<https://pypi.org/project/pymongocrypt/>`_::

  $ python3 -m pip install "pymongo[encryption]"

You can install all dependencies automatically with the following
command::

  $ python3 -m pip install "pymongo[gssapi,aws,ocsp,snappy,srv,zstd,encryption]"

Installing from source
----------------------

If you'd rather install directly from the source (i.e. to stay on the
bleeding edge), install the C extension dependencies then check out the
latest source from GitHub and install the driver from the resulting tree::

  $ git clone https://github.com/mongodb/mongo-python-driver.git pymongo
  $ cd pymongo/
  $ python3 setup.py install

Installing from source on Unix
..............................

To build the optional C extensions on Linux or another non-macOS Unix you must
have the GNU C compiler (gcc) installed. Depending on your flavor of Unix
(or Linux distribution) you may also need a python development package that
provides the necessary header files for your version of Python. The package
name may vary from distro to distro.

Debian and Ubuntu users should issue the following command::

  $ sudo apt-get install build-essential python-dev

Users of Red Hat based distributions (RHEL, CentOS, Amazon Linux, Oracle Linux,
Fedora, etc.) should issue the following command::

  $ sudo yum install gcc python-devel

Installing from source on macOS / OSX
.....................................

If you want to install PyMongo with C extensions from source you will need
the command line developer tools. On modern versions of macOS they can be
installed by running the following in Terminal (found in
/Applications/Utilities/)::

  xcode-select --install

For older versions of OSX you may need Xcode. See the notes below for various
OSX and Xcode versions.

**Snow Leopard (10.6)** - Xcode 3 with 'UNIX Development Support'.

**Snow Leopard Xcode 4**: The Python versions shipped with OSX 10.6.x
are universal binaries. They support i386, PPC, and x86_64. Xcode 4 removed
support for PPC, causing the distutils version shipped with Apple's builds of
Python to fail to build the C extensions if you have Xcode 4 installed. There
is a workaround::

  # For some Python builds from python.org
  $ env ARCHFLAGS='-arch i386 -arch x86_64' python -m easy_install pymongo

See `http://bugs.python.org/issue11623 <http://bugs.python.org/issue11623>`_
for a more detailed explanation.

**Lion (10.7) and newer** - PyMongo's C extensions can be built against
versions of Python 3.6.2+ downloaded from python.org. In all cases Xcode must be
installed with 'UNIX Development Support'.

**Xcode 5.1**: Starting with version 5.1 the version of clang that ships with
Xcode throws an error when it encounters compiler flags it doesn't recognize.
This may cause C extension builds to fail with an error similar to::

  clang: error: unknown argument: '-mno-fused-madd' [-Wunused-command-line-argument-hard-error-in-future]

There are workarounds::

  # Apple specified workaround for Xcode 5.1
  # easy_install
  $ ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future easy_install pymongo
  # or pip
  $ ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install pymongo

  # Alternative workaround using CFLAGS
  # easy_install
  $ CFLAGS=-Qunused-arguments easy_install pymongo
  # or pip
  $ CFLAGS=-Qunused-arguments pip install pymongo


Installing from source on Windows
.................................

If you want to install PyMongo with C extensions from source the following
requirements apply to both CPython and ActiveState's ActivePython:

Windows
~~~~~~~

Install Visual Studio 2015+.

.. _install-no-c:

Installing Without C Extensions
-------------------------------

By default, the driver attempts to build and install optional C
extensions (used for increasing performance) when it is installed. If
any extension fails to build the driver will be installed anyway but a
warning will be printed.

If you wish to install PyMongo without the C extensions, even if the
extensions build properly, it can be done using a command line option to
*setup.py*::

  $ python3 setup.py --no_ext install

Installing a beta or release candidate
--------------------------------------

MongoDB, Inc. may occasionally tag a beta or release candidate for testing by
the community before final release. These releases will not be uploaded to pypi
but can be found on the
`GitHub tags page <https://github.com/mongodb/mongo-python-driver/tags>`_.
They can be installed by passing the full URL for the tag to pip::

  $ python3 -m pip install https://github.com/mongodb/mongo-python-driver/archive/3.11.0rc0.tar.gz
