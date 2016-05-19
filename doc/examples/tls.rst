TLS/SSL and PyMongo 2.x
=======================

PyMongo supports connecting to MongoDB over TLS/SSL. This guide covers the
configuration options supported by PyMongo. See `the server documentation
<http://docs.mongodb.org/manual/tutorial/configure-ssl/>`_ to configure
MongoDB.

To make a secure TLS connection create
:class:`~pymongo.mongo_client.MongoClient`
(or :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`)
with the following options::

  >>> import ssl
  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                              ssl_ca_certs='/path/to/ca.pem')

Or, in the URI::

  >>> uri = 'mongodb://example.com/?ssl=true&ssl_cert_reqs=CERT_REQUIRED&ssl_ca_certs=/path/to/ca.pem'
  >>> client = pymongo.MongoClient(uri)

To verify server certificates signed by a well known certificate authority, use
`certifi <https://pypi.python.org/pypi/certifi>`_::

  >>> import certifi
  >>> import ssl
  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                              ssl_ca_certs=certifi.where())
  >>>
  >>> uri = 'mongodb://example.com/?ssl=true&ssl_cert_reqs=CERT_REQUIRED&ssl_ca_certs=%s' % (certifi.where(),)
  >>> client = pymongo.MongoClient(uri)

Client certificates
...................

PyMongo can be configured to present a client certificate using the
`ssl_certfile` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                              ssl_ca_certs='/path/to/ca.pem',
  ...                              ssl_certfile='/path/to/client.pem')

If the private key for the client certificate is stored in a separate file use
the `ssl_keyfile` option::

  >>> client = pymongo.MongoClient('example.com',
  ...                              ssl=True,
  ...                              ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                              ssl_ca_certs='/path/to/ca.pem',
  ...                              ssl_certfile='/path/to/client.pem',
  ...                              ssl_keyfile='/path/to/key.pem')

These options can also be passed as part of the MongoDB URI.
