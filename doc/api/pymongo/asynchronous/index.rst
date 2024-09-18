:mod:`pymongo async` -- Async Python driver for MongoDB
=======================================================

.. warning:: This API is currently in beta, meaning the classes, methods,
   and behaviors described within may change before the full release.
   If you come across any bugs during your use of this API,
   please file a Jira ticket in the "Python Driver" project at http://jira.mongodb.org/browse/PYTHON.

.. automodule:: pymongo.asynchronous
   :synopsis: Asynchronous Python driver for MongoDB

   .. data:: AsyncMongoClient

      Alias for :class:`pymongo.asynchronous.mongo_client.MongoClient`.

Sub-modules:

.. toctree::
   :maxdepth: 2

   change_stream
   client_session
   collection
   command_cursor
   cursor
   database
   mongo_client
