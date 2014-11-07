Requests
========

The ``start_request`` method of :class:`~pymongo.mongo_client.MongoClient`
and :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient` is now
**deprecated** and will be removed in PyMongo 3.0.

PyMongo versions previous to 3.0 support the idea of a *request*: a series of
operations executed with a single socket. This feature intended to make
read-your-writes consistency more likely, even with unacknowledged writes.
(That is, operations with write concern ``w=0``.)

However, mongos 2.6 doesn't support socket pinning by default, and `mongos 2.8
doesn't support it at all`_, so requests provide no benefit with sharding.

In any case, requests are no longer necessary with PyMongo.
By default, the methods :meth:`~pymongo.collection.Collection.insert`,
:meth:`~pymongo.collection.Collection.update`,
:meth:`~pymongo.collection.Collection.save`, and
:meth:`~pymongo.collection.Collection.remove` block until they receive
acknowledgment from the server, so ordered execution is already guaranteed. You
can be certain the next :meth:`~pymongo.collection.Collection.find` or
:meth:`~pymongo.collection.Collection.count`, for example, is executed on the
server after the writes complete. This is called "read-your-writes
consistency." If your application requires this consistency, do not override
the default write concern with ``w=0``.

.. _mongos 2.8 doesn't support it at all: https://jira.mongodb.org/browse/SERVER-12273
