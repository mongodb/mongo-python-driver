Using PyMongo with MongoDB Atlas
================================

`Atlas <https://www.mongodb.com/cloud>`_ is MongoDB, Inc.'s hosted MongoDB as a
service offering. To connect to Atlas, pass the connection string provided by
Atlas to :class:`~pymongo.mongo_client.MongoClient`::

  client = pymongo.MongoClient(<Atlas connection string>)

Connections to Atlas require TLS/SSL. For connections using TLS/SSL, PyMongo
may require third party dependencies as determined by your version of Python.
With PyMongo 3.3+, you can install PyMongo 3.3+ and any TLS/SSL-related
dependencies using the following pip command::

  $ python -m pip install pymongo[tls]

Earlier versions of PyMongo require you to manually install the dependencies.
For a list of TLS/SSL-related dependencies, see :doc:`examples/tls`.
