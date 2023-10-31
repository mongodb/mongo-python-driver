
.. _network-compression-example:

Network Compression
===================

PyMongo supports network compression where network traffic between the client
and MongoDB server are compressed which reduces the amount of data passed
over the network. By default no compression is used.

The driver supports the following algorithms:

- `snappy <https://pypi.org/project/python-snappy>`_ available in MongoDB 3.4 and later.
- :mod:`zlib` available in MongoDB 3.6 and later.
- `zstandard <https://pypi.org/project/zstandard/>`_ available in MongoDB 4.2 and later.

.. note:: snappy and zstandard compression require additional dependencies. See :ref:`optional-deps`.

Applications can enable wire protocol compression via the ``compressors`` URI and
keyword argument to :meth:`~pymongo.mongo_client.MongoClient`. For example::

  >>> client = MongoClient(compressors='zlib')
  >>> client.test.test.insert_one({'data': 's'*1*1024*1024})  # Will be compressed with zlib.

When multiple compression algorithms are given, the driver selects the first one in the
list supported by the MongoDB instance to which it is connected. For example::

  >>> client = MongoClient(compressors='snappy,zlib')
  >>> client.test.test.insert_one({'data': 's'*1*1024*1024})  # Will be compressed with snappy.

The ``compressors`` option can also be set via the URI::

  >>> client = MongoClient('mongodb://example.com/?compressors=zstandard,snappy,zlib')
