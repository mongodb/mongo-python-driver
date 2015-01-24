Authentication Examples
=======================

MongoDB supports several different authentication mechanisms. These examples
cover all authentication methods currently supported by PyMongo, documenting
Python module and MongoDB version dependencies.

Support For Special Characters In Usernames And Passwords
---------------------------------------------------------

If your username or password contains special characters (e.g. '/', ' ',
or '@') you must ``%xx`` escape them for use in the MongoDB URI. PyMongo
uses :meth:`~urllib.unquote_plus` to decode them. For example::

  >>> import urllib
  >>> password = urllib.quote_plus('pass/word')
  >>> password
  'pass%2Fword'
  >>> MongoClient('mongodb://user:' + password + '@127.0.0.1')
  MongoClient('127.0.0.1', 27017)

SCRAM-SHA-1 (RFC 5802)
----------------------
.. versionadded:: 2.8

SCRAM-SHA-1 is the default authentication mechanism supported by a cluster
configured for authentication with MongoDB 3.0 or later. Authentication is
per-database and credentials can be specified through the MongoDB URI or
passed to the :meth:`~pymongo.database.Database.authenticate` method::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com')
  >>> client.the_database.authenticate('user', 'password', mechanism='SCRAM-SHA-1')
  True
  >>>
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
  >>> client = MongoClient('example.com')
  >>> client.the_database.authenticate('user', 'password', mechanism='MONGODB-CR')
  True
  >>>
  >>> uri = "mongodb://user:password@example.com/the_database?authMechanism=MONGODB-CR"
  >>> client = MongoClient(uri)

Default Authentication Mechanism
--------------------------------

If no mechanism is specified, PyMongo automatically uses MONGODB-CR when
connected to a pre-3.0 version of MongoDB, and SCRAM-SHA-1 when connected to
a recent version.

Delegated Authentication
------------------------
.. versionadded: 2.5

In MongoDB 2.4.x a separate authentication source can be specified.
This feature was introduced in MongoDB 2.4 and removed in 2.6::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com')
  >>> client.the_database.authenticate('user',
  ...                                  'password',
  ...                                  source='source_database')
  True
  >>>
  >>> uri = "mongodb://user:password@example.com/?authSource=source_database"
  >>> client = MongoClient(uri)
  >>>

MONGODB-X509
------------
.. versionadded:: 2.6

The MONGODB-X509 mechanism authenticates a username derived from the
distinguished subject name of the X.509 certificate presented by the driver
during SSL negotiation. This authentication method requires the use of SSL
connections with certificate validation and is available in MongoDB 2.5.1
and newer::

  >>> import ssl
  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                       ssl=True,
  ...                       ssl_certfile='/path/to/client.pem',
  ...                       ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                       ssl_ca_certs='/path/to/ca.pem')
  >>> client.the_database.authenticate("<X.509 derived username>",
  ...                                  mechanism='MONGODB-X509')
  True
  >>>

MONGODB-X509 authenticates against the $external virtual database, so you
do not have to specify a database in the URI::

  >>> uri = "mongodb://<X.509 derived username>@example.com/?authMechanism=MONGODB-X509"
  >>> client = MongoClient(uri,
  ...                     ssl=True,
  ...                     ssl_certfile='/path/to/client.pem',
  ...                     ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                     ssl_ca_certs='/path/to/ca.pem')
  >>>

.. _use_kerberos:

GSSAPI (Kerberos)
-----------------
.. versionadded:: 2.5

GSSAPI (Kerberos) authentication is available in the Enterprise Edition of
MongoDB, version 2.4 and newer. To authenticate using GSSAPI you must first
install the python `kerberos`_ or `pykerberos`_ module using easy_install or
pip. Make sure you run kinit before using the following authentication methods::

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

or using :meth:`~pymongo.database.Database.authenticate`::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com')
  >>> db = client.test
  >>> db.authenticate('mongodbuser@EXAMPLE.COM', mechanism='GSSAPI')
  True

The default service name used by MongoDB and PyMongo is `mongodb`. You can
specify a custom service name with the ``authMechanismProperties`` option::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@example.com/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:myservicename"
  >>> client = MongoClient(uri)
  >>>
  >>> client = MongoClient('example.com')
  >>> db = client.test
  >>> db.authenticate(
  ...     'mongodbuser@EXAMPLE.COM', mechanism='GSSAPI',
  ...     authMechanismProperties='SERVICE_NAME:myservicename')
  True

.. note::
   Kerberos support is only provided in environments supported by the python
   `kerberos`_ or `pykerberos`_ modules. This currently limits support to
   CPython and Unix environments.

.. _kerberos: http://pypi.python.org/pypi/kerberos
.. _pykerberos: https://pypi.python.org/pypi/pykerberos

SASL PLAIN (RFC 4616)
---------------------
.. versionadded:: 2.6

MongoDB Enterprise Edition versions 2.5.0 and newer support the SASL PLAIN
authentication mechanism, initially intended for delegating authentication
to an LDAP server. Using the PLAIN mechanism is very similar to MONGODB-CR.
These examples use the $external virtual database for LDAP support::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com')
  >>> client.the_database.authenticate('user',
  ...                                  'password',
  ...                                  source='$external',
  ...                                  mechanism='PLAIN')
  True
  >>>
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN&authSource=$external"
  >>> client = MongoClient(uri)
  >>>

SASL PLAIN is a clear-text authentication mechanism. We **strongly** recommend
that you connect to MongoDB using SSL with certificate validation when using
the SASL PLAIN mechanism::

  >>> import ssl
  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      ssl=True,
  ...                      ssl_certfile='/path/to/client.pem',
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs='/path/to/ca.pem')
  >>> client.the_database.authenticate('user',
  ...                                  'password',
  ...                                  source='$external',
  ...                                  mechanism='PLAIN')
  True
  >>>
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN&authSource=$external"
  >>> client = MongoClient(uri,
  ...                      ssl=True,
  ...                      ssl_certfile='/path/to/client.pem',
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs='/path/to/ca.pem')
  >>>

