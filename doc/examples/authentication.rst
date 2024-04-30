Authentication Examples
=======================

MongoDB supports several different authentication mechanisms. These examples
cover all authentication methods currently supported by PyMongo, documenting
Python module and MongoDB version dependencies.

.. _percent escaped:

Percent-Escaping Username and Password
--------------------------------------

Username and password must be percent-escaped with
:py:func:`urllib.parse.quote_plus`, to be used in a MongoDB URI. For example::

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

If no mechanism is specified, PyMongo automatically SCRAM-SHA-1 when connected
to MongoDB 3.6 and negotiates the mechanism to use (SCRAM-SHA-1
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

The MONGODB-X509 mechanism authenticates via the X.509 certificate presented
by the driver during TLS/SSL negotiation. This authentication method requires
the use of TLS/SSL connections with certificate validation::

  >>> from pymongo import MongoClient
  >>> client = MongoClient('example.com',
  ...                      authMechanism="MONGODB-X509",
  ...                      tls=True,
  ...                      tlsCertificateKeyFile='/path/to/client.pem',
  ...                      tlsCAFile='/path/to/ca.pem')

MONGODB-X509 authenticates against the $external virtual database, so you
do not have to specify a database in the URI::

  >>> uri = "mongodb://example.com/?authMechanism=MONGODB-X509"
  >>> client = MongoClient(uri,
  ...                      tls=True,
  ...                      tlsCertificateKeyFile='/path/to/client.pem',
  ...                      tlsCAFile='/path/to/ca.pem')
  >>>

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

The default service name used by MongoDB and PyMongo is ``mongodb``. You can
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
`credentials`_ is the same as the one used by the AWS ``boto3`` library
when using ``pymongo_auth_aws>=1.1.0``.

Because we are now using ``boto3`` to handle credentials, the order and
locations of credentials are slightly different from before.  Particularly,
if you have a shared AWS credentials or config file,
then those credentials will be used by default if AWS auth environment
variables are not set.  To override this behavior, set
``AWS_SHARED_CREDENTIALS_FILE=""`` in your shell or add
``os.environ["AWS_SHARED_CREDENTIALS_FILE"] = ""`` to your script or
application.  Alternatively, you can create an AWS profile specifically for
your MongoDB credentials and set ``AWS_PROFILE`` to that profile name.

MONGODB-AWS authenticates against the "$external" virtual database, so none of
the URIs in this section need to include the ``authSource`` URI option.

.. _credentials: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

AWS IAM credentials
~~~~~~~~~~~~~~~~~~~

Applications can authenticate using AWS IAM credentials by providing a valid
access key id and secret access key pair as the username and password,
respectively, in the MongoDB URI. A sample URI would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb+srv://<access_key_id>:<secret_access_key>@example.mongodb.net/?authMechanism=MONGODB-AWS"
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
  >>> uri = "mongodb+srv://<access_key_id>:<secret_access_key>@example.mongodb.net/?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:<session_token>"
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
  >>> uri = "mongodb+srv://example.mongodb.net/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: No username, password, or session token is passed into the URI.
          PyMongo will use credentials set via the environment variables.
          These environment variables MUST NOT be `percent escaped`_.


.. _EKS Clusters:

EKS Clusters
~~~~~~~~~~~~

Applications using the `Authenticating users for your cluster from an OpenID Connect identity provider <https://docs.aws.amazon.com/eks/latest/userguide/authenticate-oidc-identity-provider.html>`_ capability on EKS can now
use the provided credentials, by giving the associated IAM User
`sts:AssumeRoleWithWebIdentity <https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html>`_
permission.

When the username and password are not provided, the MONGODB-AWS mechanism
is set, and ``AWS_WEB_IDENTITY_TOKEN_FILE``, ``AWS_ROLE_ARN``, and
optional ``AWS_ROLE_SESSION_NAME`` are available, the driver will use
an ``AssumeRoleWithWebIdentity`` call to retrieve temporary credentials.
The application must be using ``pymongo_auth_aws`` >= 1.1.0 for EKS support.

ECS Container
~~~~~~~~~~~~~

Applications can authenticate from an ECS container via temporary
credentials assigned to the machine. A sample URI on an ECS container
would be::

  >>> from pymongo import MongoClient
  >>> uri = "mongodb+srv://example.mongodb.com/?authMechanism=MONGODB-AWS"
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
  >>> uri = "mongodb+srv://example.mongodb.com/?authMechanism=MONGODB-AWS"
  >>> client = MongoClient(uri)

.. note:: No username, password, or session token is passed into the URI.
          PyMongo will query the EC2 instance endpoint to obtain these
          credentials.

.. _temporary AWS IAM credentials: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html
.. _AWS Security Token Service (STS): https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html
.. _Assume Role: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
.. _EC2 instance: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html
.. _environment variables: https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime

MONGODB-OIDC
------------
.. versionadded:: 4.7

The `MONGODB-OIDC authentication mechanism`_ is available in MongoDB 7.0+ on Linux platforms.

The MONGODB-OIDC mechanism authenticates using an OpenID Connect (OIDC) access token.
The driver supports OIDC for workload identity, defined as an identity you assign to a software workload
(such as an application, service, script, or container) to authenticate and access other services and resources.

Credentials can be configured through the MongoDB URI or as arguments to
:class:`~pymongo.mongo_client.MongoClient`.

Built-in Support
~~~~~~~~~~~~~~~~

The driver has built-in support for Azure IMDS and GCP IMDS environments.  Other environments
are supported with `Custom Callbacks`_.

Azure IMDS
^^^^^^^^^^

For an application running on an Azure VM or otherwise using the `Azure Internal Metadata Service`_,
you can use the built-in support for Azure. If using an Azure managed identity, the "<client_id>" is
the client ID.  If using a service principal to represent an enterprise application, the "<client_id>" is
the application ID of the service principal. The ``<audience>`` value is the ``audience``
`configured on your MongoDB deployment`_.

.. code-block:: python

    import os

    uri = os.environ["MONGODB_URI"]

    props = {"ENVIRONMENT": "azure", "TOKEN_RESOURCE": "<audience>"}
    c = MongoClient(
        uri,
        username="<client_id>",
        authMechanism="MONGODB-OIDC",
        authMechanismProperties=props,
    )
    c.test.test.insert_one({})
    c.close()

If the application is running on an Azure VM and only one managed identity is associated with the
VM, ``username`` can be omitted.

If providing the ``TOKEN_RESOURCE`` as part of a connection string, it can be given as follows.
If the ``TOKEN_RESOURCE`` contains any of the following characters [``,``, ``+``, ``&``], then
it MUST be url-encoded.

.. code-block:: python

    import os

    uri = f'{os.environ["MONGODB_URI"]}?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:azure,TOKEN_RESOURCE:<audience>'
    c = MongoClient(uri)
    c.test.test.insert_one({})
    c.close()

GCP IMDS
^^^^^^^^

For an application running on an GCP VM or otherwise using the `GCP Internal Metadata Service`_,
you can use the built-in support for GCP, where ``<audience>`` below is the ``audience``
`configured on your MongoDB deployment`_.

.. code-block:: python

    import os

    uri = os.environ["MONGODB_URI"]

    props = {"ENVIRONMENT": "gcp", "TOKEN_RESOURCE": "<audience>"}
    c = MongoClient(uri, authMechanism="MONGODB-OIDC", authMechanismProperties=props)
    c.test.test.insert_one({})
    c.close()

If providing the ``TOKEN_RESOURCE`` as part of a connection string, it can be given as follows.
If the ``TOKEN_RESOURCE`` contains any of the following characters [``,``, ``+``, ``&``], then
it MUST be url-encoded.

.. code-block:: python

    import os

    uri = f'{os.environ["MONGODB_URI"]}?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:<audience>'
    c = MongoClient(uri)
    c.test.test.insert_one({})
    c.close()

Custom Callbacks
~~~~~~~~~~~~~~~~

For environments that are not directly supported by the driver, you can use :class:`~pymongo.auth_oidc.OIDCCallback`.
Some examples are given below.

Other Azure Environments
^^^^^^^^^^^^^^^^^^^^^^^^

For applications running on Azure Functions, App Service Environment (ASE), or
Azure Kubernetes Service (AKS), you can use the `azure-identity package`_
to fetch the credentials.  This example assumes you have set environment variables for
the ``audience`` `configured on your MongoDB deployment`_, and for the client id of the Azure
managed identity.

.. code-block:: python

  import os
  from azure.identity import DefaultAzureCredential
  from pymongo import MongoClient
  from pymongo.auth_oidc import OIDCCallback, OIDCCallbackContext, OIDCCallbackResult

  audience = os.environ["AZURE_AUDIENCE"]
  client_id = os.environ["AZURE_IDENTITY_CLIENT_ID"]
  uri = os.environ["MONGODB_URI"]


  class MyCallback(OIDCCallback):
      def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
          credential = DefaultAzureCredential(managed_identity_client_id=client_id)
          token = credential.get_token(f"{audience}/.default").token
          return OIDCCallbackResult(access_token=token)


  props = {"OIDC_CALLBACK": MyCallback()}
  c = MongoClient(uri, authMechanism="MONGODB-OIDC", authMechanismProperties=props)
  c.test.test.insert_one({})
  c.close()

GCP GKE
^^^^^^^

For a Google Kubernetes Engine cluster with a `configured service account`_, the token can be read from the standard
service account token file location.

.. code-block:: python

    import os
    from pymongo.auth_oidc import OIDCCallback, OIDCCallbackContext, OIDCCallbackResult


    class MyCallback(OIDCCallback):
        def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as fid:
                token = fid.read()
            return OIDCCallbackResult(access_token=token)


    uri = os.environ["MONGODB_URI"]
    props = {"OIDC_CALLBACK": MyCallback()}
    c = MongoClient(uri, authMechanism="MONGODB-OIDC", authMechanismProperties=props)
    c.test.test.insert_one({})
    c.close()

.. _MONGODB-OIDC authentication mechanism: https://www.mongodb.com/docs/manual/core/security-oidc/
.. _Azure Internal Metadata Service: https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service
.. _configured on your MongoDB deployment: https://www.mongodb.com/docs/manual/reference/parameters/#mongodb-parameter-param.oidcIdentityProviders
.. _GCP Internal Metadata Service: https://cloud.google.com/compute/docs/metadata/querying-metadata
.. _azure-identity package: https://pypi.org/project/azure-identity/
.. _configured service account: https://cloud.google.com/kubernetes-engine/docs/how-to/service-accounts
