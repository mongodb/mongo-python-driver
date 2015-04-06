TLS/SSL and PyMongo
===================

PyMongo supports connecting to MongoDB over TLS/SSL. This guide covers the
configuration options supported by PyMongo. See `the server documentation
<http://docs.mongodb.org/manual/tutorial/configure-ssl/>`_ to configure
MongoDB.

Basic configuration
...................

In many cases connecting to MongoDB over TLS/SSL requires nothing more than
passing ``ssl=True`` as a keyword argument to
:class:`~pymongo.mongo_client.MongoClient`::

  >>> client = pymongo.MongoClient('example.com', ssl=True)

Or passing ``ssl=true`` in the URI::

  >>> client = pymongo.MongoClient('mongodb://example.com/?ssl=true')

This configures PyMongo to connect to the server using TLS, verify the server's
certificate and verify that the host you are attempting to connect to is listed
by that certificate.

PyMongo attempts to use the operating system's CA certificates to verify the
server's certificate when possible. Some versions of python may require an
extra third party module for this to work properly. Users of Python 2 on
Windows are encouraged to upgrade to python 2.7.9 or newer. Users of Python 3
on Windows should upgrade to python 3.4.0 or newer. If upgrading is not
possible `wincertstore <https://pypi.python.org/pypi/wincertstore>`_ can be
used with older python versions. Users of operating systems other than Windows
that are stuck on python versions older than 2.7.9 can install
`certifi <https://pypi.python.org/pypi/certifi>`_ to use the Mozilla CA bundle
for certificate verification.

Certificate verification policy
...............................

By default, PyMongo is configured to require a certificate from the server when
TLS is enabled. This is configurable using the `ssl_cert_reqs` option. To
disable this requirement pass ``ssl.CERT_NONE`` as a keyword parameter::

  >>> import ssl
  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_NONE)

Or, in the URI::

  >>> uri = 'mongodb://example.com/?ssl=true&ssl_cert_reqs=CERT_NONE'
  >>> client = pymongo.MongoClient(uri)

You can also configure optional certificate verification, if a certificate is
provided by the server::

  >>> import ssl
  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_OPTIONAL)
  >>>
  >>> uri = 'mongodb://example.com/?ssl=true&ssl_cert_reqs=CERT_OPTIONAL'
  >>> client = pymongo.MongoClient(uri)

Specifying a CA file
....................

In some cases you may want to configure PyMongo to use a specific set of CA
certificates. This is most often the case when using "self-signed" server
certificates. The `ssl_ca_certs` option takes a path to a CA file. It can be
passed as a keyword argument::

  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_ca_certs='/path/to/ca.pem')

Or, in the URI::

  >>> uri = 'mongodb://example.com/?ssl=true&ssl_ca_certs=/path/to/ca.pem'
  >>> client = pymongo.MongoClient(uri)

Client certificates
...................

PyMongo can be configured to present a client certificate using the
`ssl_certfile` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_certfile='/path/to/client.pem')

If the private key for the client certificate is stored in a separate file use
the `ssl_keyfile` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_certfile='/path/to/client.pem',
  ...                              ssl_keyfile='/path/to/key.pem')

These options can also be passed as part of the MongoDB URI.
