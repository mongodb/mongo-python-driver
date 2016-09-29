Using PyMongo 2.x with MongoDB Atlas
====================================

`Atlas <https://www.mongodb.com/cloud>`_ is MongoDB, Inc's hosted MongoDB as a
service offering. The following steps are required to securely connect to Atlas
with PyMongo 2.x.

.. warning:: These directions **MUST** be followed carefully to ensure a secure
  connection is used.

First, install `certifi <https://pypi.python.org/pypi/certifi>`_::

  $ python -m pip install certifi

To connect to Atlas, pass the connection string provided by Atlas to
:class:`~pymongo.mongo_client.MongoClient`. You **MUST** provide all of these
options to make a secure connection::

  >>> import certifi
  >>> import ssl
  >>> from pymongo import MongoClient
  >>> client = MongoClient(<Atlas connection string>,
  ...                      ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                      ssl_ca_certs=certifi.where())

Connections to Atlas using
:class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` require the
same options::

  >>> import certifi
  >>> import ssl
  >>> from pymongo import MongoReplicaSetClient
  >>> client = MongoReplicaSetClient(<Atlas connection string>,
  ...                                ssl_cert_reqs=ssl.CERT_REQUIRED,
  ...                                ssl_ca_certs=certifi.where())
