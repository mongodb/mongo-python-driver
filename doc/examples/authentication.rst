Authentication Examples
=======================

MongoDB supports several different authentication mechanisms. These examples
cover all authentication methods currently supported by PyMongo, documenting
Python module and MongoDB version dependencies.

Percent-Escaping Username and Password
--------------------------------------

Username and password must be percent-escaped with
:meth:`urllib.parse.quote_plus` in Python 3, or :meth:`urllib.quote_plus` in
Python 2, to be used in a MongoDB URI. For example, in Python 3::

  >>> from pymongo import MongoClient
  >>> import urllib.parse
  >>> username = urllib.parse.quote_plus('user')
  >>> username
  'user'
  >>> password = urllib.parse.quote_plus('pass/word')
  >>> password
  'pass%2Fword'
  >>> MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
  ...

SCRAM-SHA-1 (RFC 5802)
----------------------
.. versionadded:: 2.8

SCRAM-SHA-1 is the default authentication mechanism supported by a cluster
configured for authentication with MongoDB 3.0 or later. Authentication
requires a username, a password, and a database name. The default database
name is "admin", this can be overidden with the ``authSource`` option.
Credentials can be specified as arguments to
:class:`~pymongo.mongo_client.MongoClient`::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      user='user',
  ...                      password='password',
  ...                      authSource='the_database',
  ...                      authMechanism='SCRAM-SHA-1')

Or through the MongoDB URI::

  >>> uri = "mongodb://user:password@example.com/the_database?authMechanism=SCRAM-SHA-1"
  >>> client = MongoClient(uri)

For best performance install `backports.pbkdf2`_, especially on Python older
than 2.7.8, or on Python 3 before Python 3.4.

.. _backports.pbkdf2: https://pypi.python.org/pypi/backports.pbkdf2/

MONGODB-CR
----------

Before MongoDB 3.0 the default authentication mechanism was MONGODB-CR,
the "MongoDB Challenge-Response" protocol::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      user='user',
  ...                      password='password',
  ...                      authMechanism='MONGODB-CR')
  >>>
  >>> uri = "mongodb://user:password@example.com/the_database?authMechanism=MONGODB-CR"
  >>> client = MongoClient(uri)

Default Authentication Mechanism
--------------------------------

If no mechanism is specified, PyMongo automatically uses MONGODB-CR when
connected to a pre-3.0 version of MongoDB, and SCRAM-SHA-1 when connected to
a recent version.

Default Database and "authSource"
---------------------------------

You can specify both a default database and the authentication database in the
URI::

    >>> uri = "mongodb://user:password@example.com/default_db?authSource=admin"
    >>> client = MongoClient(uri)

PyMongo will authenticate on the "admin" database, but the default database
will be "default_db"::

    >>> # get_database with no "name" argument chooses the DB from the URI
    >>> db = MongoClient(uri).get_database()
    >>> print(db.name)
    'default_db'

MONGODB-X509
------------
.. versionadded:: 2.6

The MONGODB-X509 mechanism authenticates a username derived from the
distinguished subject name of the X.509 certificate presented by the driver
during SSL negotiation. This authentication method requires the use of SSL
connections with certificate validation and is available in MongoDB 2.6
and newer::

  >>> import ssl
  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      username="<X.509 derived username>"
  ...                      authMechanism="MONGODB-X509",
  ...                      ssl=True,
  ...                      ssl_certfile='/path/to/client.pem',
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs='/path/to/ca.pem')

MONGODB-X509 authenticates against the $external virtual database, so you
do not have to specify a database in the URI::

  >>> uri = "mongodb://<X.509 derived username>@example.com/?authMechanism=MONGODB-X509"
  >>> client = MongoClient(uri,
  ...                      ssl=True,
  ...                      ssl_certfile='/path/to/client.pem',
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs='/path/to/ca.pem')
  >>>

.. versionchanged:: 3.4
  When connected to MongoDB >= 3.4 the username is no longer required.

.. _use_kerberos:

GSSAPI (Kerberos)
-----------------
.. versionadded:: 2.5

GSSAPI (Kerberos) authentication is available in the Enterprise Edition of
MongoDB.

Unix
~~~~

To authenticate using GSSAPI you must first install the python `kerberos`_ or
`pykerberos`_ module using easy_install or pip. Make sure you run kinit before
using the following authentication methods::

  $ kinit mongodbuser@EXAMPLE.COM
  mongodbuser@EXAMPLE.COM's Password: 
  $ klist
  Credentials cache: FILE:/tmp/krb5cc_1000
          Principal: mongodbuser@EXAMPLE.COM

    Issued                Expires               Principal
  Feb  9 13:48:51 2013  Feb  9 23:48:51 2013  krbtgt/EXAMPLE.COM@EXAMPLE.COM

Now authenticate using the MongoDB URI. GSSAPI authenticates against the
$external virtual database so you do not have to specify a database in the
URI::

  >>> # Note: the kerberos principal must be url encoded.
  >>> from pymongo import MongoClient
  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@example.com/?authMechanism=GSSAPI"
  >>> client = MongoClient(uri)
  >>>

The default service name used by MongoDB and PyMongo is `mongodb`. You can
specify a custom service name with the ``authMechanismProperties`` option::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@example.com/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:myservicename"
  >>> client = MongoClient(uri)

Windows (SSPI)
~~~~~~~~~~~~~~
.. versionadded:: 3.3

First install the `winkerberos`_ module. Unlike authentication on Unix kinit is
not used. If the user to authenticate is different from the user that owns the
application process provide a password to authenticate::

  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM:mongodbuserpassword@example.com/?authMechanism=GSSAPI"

Two extra ``authMechanismProperties`` are supported on Windows platforms:

- CANONICALIZE_HOST_NAME - Uses the fully qualified domain name (FQDN) of the
  MongoDB host for the server principal (GSSAPI libraries on Unix do this by
  default)::

    >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@example.com/?authMechanism=GSSAPI&authMechanismProperties=CANONICALIZE_HOST_NAME:true"

- SERVICE_REALM - This is used when the user's realm is different from the service's realm::

    >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@example.com/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_REALM:otherrealm"


.. _kerberos: http://pypi.python.org/pypi/kerberos
.. _pykerberos: https://pypi.python.org/pypi/pykerberos
.. _winkerberos: https://pypi.python.org/pypi/winkerberos/

SASL PLAIN (RFC 4616)
---------------------
.. versionadded:: 2.6

MongoDB Enterprise Edition version 2.6 and newer support the SASL PLAIN
authentication mechanism, initially intended for delegating authentication
to an LDAP server. Using the PLAIN mechanism is very similar to MONGODB-CR.
These examples use the $external virtual database for LDAP support::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN&authSource=$external"
  >>> client = MongoClient(uri)
  >>>

SASL PLAIN is a clear-text authentication mechanism. We **strongly** recommend
that you connect to MongoDB using SSL with certificate validation when using
the SASL PLAIN mechanism::

  >>> import ssl
  >>> from pymongo import MongoClient
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN&authSource=$external"
  >>> client = MongoClient(uri,
  ...                      ssl=True,
  ...                      ssl_certfile='/path/to/client.pem',
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs='/path/to/ca.pem')
  >>>
