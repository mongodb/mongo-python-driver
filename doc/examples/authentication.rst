Authentication Examples
=======================

MongoDB supports several different authentication mechanisms. These examples
cover all authentication methods currently supported by PyMongo, documenting
Python module and MongoDB version dependencies.

.. _percent escaped:

Percent-Escaping Username and Password
--------------------------------------

Username and password must be percent-escaped with
:py:func:`urllib.parse.quote`, to be used in a MongoDB URI. For example::

  >>> from pymongo import MongoClient
  >>> import urllib.parse
  >>> username = urllib.parse.quote('user')
  >>> username
  'user'
  >>> password = urllib.parse.quote('pass/word')
  >>> password
  'pass%2Fword'
  >>> MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
  ...

.. _scram_sha_256:

SCRAM-SHA-256 (RFC 7677)
------------------------
.. versionadded:: 3.7

SCRAM-SHA-256 is the default authentication mechanism supported by a cluster
configured for authentication with MongoDB 4.0 or later. Authentication
requires a username, a password, and a database name. The default database
name is "admin", this can be overridden with the ``authSource`` option.
Credentials can be specified as arguments to
:class:`~pymongo.mongo_client.MongoClient`::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      username='user',
  ...                      password='password',
  ...                      authSource='the_database',
  ...                      authMechanism='SCRAM-SHA-256')

Or through the MongoDB URI::

  >>> uri = "mongodb://user:password@example.com/?authSource=the_database&authMechanism=SCRAM-SHA-256"
  >>> client = MongoClient(uri)

SCRAM-SHA-1 (RFC 5802)
----------------------
.. versionadded:: 2.8

SCRAM-SHA-1 is the default authentication mechanism supported by a cluster
configured for authentication with MongoDB 3.0 or later. Authentication
requires a username, a password, and a database name. The default database
name is "admin", this can be overridden with the ``authSource`` option.
Credentials can be specified as arguments to
:class:`~pymongo.mongo_client.MongoClient`::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      username='user',
  ...                      password='password',
  ...                      authSource='the_database',
  ...                      authMechanism='SCRAM-SHA-1')

Or through the MongoDB URI::

  >>> uri = "mongodb://user:password@example.com/?authSource=the_database&authMechanism=SCRAM-SHA-1"
  >>> client = MongoClient(uri)

For best performance on Python versions older than 2.7.8 install `backports.pbkdf2`_.

.. _backports.pbkdf2: https://pypi.python.org/pypi/backports.pbkdf2/

MONGODB-CR
----------

.. warning:: MONGODB-CR was deprecated with the release of MongoDB 3.6 and
  is no longer supported by MongoDB 4.0.

Before MongoDB 3.0 the default authentication mechanism was MONGODB-CR,
the "MongoDB Challenge-Response" protocol::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      username='user',
  ...                      password='password',
  ...                      authMechanism='MONGODB-CR')
  >>>
  >>> uri = "mongodb://user:password@example.com/?authSource=the_database&authMechanism=MONGODB-CR"
  >>> client = MongoClient(uri)

Default Authentication Mechanism
--------------------------------

If no mechanism is specified, PyMongo automatically uses MONGODB-CR when
connected to a pre-3.0 version of MongoDB, SCRAM-SHA-1 when connected to
MongoDB 3.0 through 3.6, and negotiates the mechanism to use (SCRAM-SHA-1
or SCRAM-SHA-256) when connected to MongoDB 4.0+.

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

.. _mongodb_x509:

MONGODB-X509
------------
.. versionadded:: 2.6

The MONGODB-X509 mechanism authenticates a username derived from the
distinguished subject name of the X.509 certificate presented by the driver
during TLS/SSL negotiation. This authentication method requires the use of
TLS/SSL connections with certificate validation and is available in
MongoDB 2.6 and newer::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      username="<X.509 derived username>"
  ...                      authMechanism="MONGODB-X509",
  ...                      tls=True,
  ...                      tlsCertificateKeyFile='/path/to/client.pem',
  ...                      tlsCAFile='/path/to/ca.pem')

MONGODB-X509 authenticates against the $external virtual database, so you
do not have to specify a database in the URI::

  >>> uri = "mongodb://<X.509 derived username>@example.com/?authMechanism=MONGODB-X509"
  >>> client = MongoClient(uri,
  ...                      tls=True,
  ...                      tlsCertificateKeyFile='/path/to/client.pem',
  ...                      tlsCAFile='/path/to/ca.pem')
  >>>

.. versionchanged:: 3.4
  When connected to MongoDB >= 3.4 the username is no longer required.

.. _gssapi:

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
  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@mongo-server.example.com/?authMechanism=GSSAPI"
  >>> client = MongoClient(uri)
  >>>

The default service name used by MongoDB and PyMongo is `mongodb`. You can
specify a custom service name with the ``authMechanismProperties`` option::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://mongodbuser%40EXAMPLE.COM@mongo-server.example.com/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:myservicename"
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

.. _sasl_plain:

SASL PLAIN (RFC 4616)
---------------------
.. versionadded:: 2.6

MongoDB Enterprise Edition version 2.6 and newer support the SASL PLAIN
authentication mechanism, initially intended for delegating authentication
to an LDAP server. Using the PLAIN mechanism is very similar to MONGODB-CR.
These examples use the $external virtual database for LDAP support::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN"
  >>> client = MongoClient(uri)
  >>>

SASL PLAIN is a clear-text authentication mechanism. We **strongly** recommend
that you connect to MongoDB using TLS/SSL with certificate validation when
using the SASL PLAIN mechanism::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://user:password@example.com/?authMechanism=PLAIN"
  >>> client = MongoClient(uri,
  ...                      tls=True,
  ...                      tlsCertificateKeyFile='/path/to/client.pem',
  ...                      tlsCAFile='/path/to/ca.pem')
  >>>

.. _MONGODB-AWS:

MONGODB-AWS
-----------
.. versionadded:: 3.11

The MONGODB-AWS authentication mechanism is available in MongoDB 4.4+ and
requires extra pymongo dependencies. To use it, install pymongo with the
``aws`` extra::

  $ python -m pip install 'pymongo[aws]'

The MONGODB-AWS mechanism authenticates using AWS IAM credentials (an access
key ID and a secret access key), `temporary AWS IAM credentials`_ obtained
from an `AWS Security Token Service (STS)`_ `Assume Role`_ request,
AWS Lambda `environment variables`_, or temporary AWS IAM credentials assigned
to an `EC2 instance`_ or ECS task. The use of temporary credentials, in
addition to an access key ID and a secret access key, also requires a
security (or session) token.

Credentials can be configured through the MongoDB URI, environment variables,
or the local EC2 or ECS endpoint. The order in which the client searches for
credentials is:

#. Credentials passed through the URI
#. Environment variables
#. ECS endpoint if and only if ``AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`` is set.
#. EC2 endpoint

MONGODB-AWS authenticates against the "$external" virtual database, so none of
the URIs in this section need to include the ``authSource`` URI option.

AWS IAM credentials
~~~~~~~~~~~~~~~~~~~

Applications can authenticate using AWS IAM credentials by providing a valid
access key id and secret access key pair as the username and password,
respectively, in the MongoDB URI. A sample URI would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://<access_key_id>:<secret_access_key>@localhost/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: The access_key_id and secret_access_key passed into the URI MUST
          be `percent escaped`_.

AssumeRole
~~~~~~~~~~

Applications can authenticate using temporary credentials returned from an
assume role request. These temporary credentials consist of an access key
ID, a secret access key, and a security token passed into the URI.
A sample URI would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://<access_key_id>:<secret_access_key>@example.com/?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:<session_token>"
  >>> client = MongoClient(uri)

.. note:: The access_key_id, secret_access_key, and session_token passed into
          the URI MUST be `percent escaped`_.

AWS Lambda (Environment Variables)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the username and password are not provided and the MONGODB-AWS mechanism
is set, the client will fallback to using the `environment variables`_
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_SESSION_TOKEN``
for the access key ID, secret access key, and session token, respectively::

  $ export AWS_ACCESS_KEY_ID=<access_key_id>
  $ export AWS_SECRET_ACCESS_KEY=<secret_access_key>
  $ export AWS_SESSION_TOKEN=<session_token>
  $ python
  >>> from pymongo import MongoClient
  >>> uri = "mongodb://example.com/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: No username, password, or session token is passed into the URI.
          PyMongo will use credentials set via the environment variables.
          These environment variables MUST NOT be `percent escaped`_.

ECS Container
~~~~~~~~~~~~~

Applications can authenticate from an ECS container via temporary
credentials assigned to the machine. A sample URI on an ECS container
would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://localhost/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: No username, password, or session token is passed into the URI.
          PyMongo will query the ECS container endpoint to obtain these
          credentials.

EC2 Instance
~~~~~~~~~~~~

Applications can authenticate from an EC2 instance via temporary
credentials assigned to the machine. A sample URI on an EC2 machine
would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb://localhost/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: No username, password, or session token is passed into the URI.
          PyMongo will query the EC2 instance endpoint to obtain these
          credentials.

.. _temporary AWS IAM credentials: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html
.. _AWS Security Token Service (STS): https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html
.. _Assume Role: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
.. _EC2 instance: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html
.. _environment variables: https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
