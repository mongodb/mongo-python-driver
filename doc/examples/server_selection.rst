Server Selector Example
=======================

Users can exert fine-grained control over the `server selection algorithm`_
by setting the `serverSelector` option on the :class:`~pymongo.MongoClient`
to an appropriate callable. This example shows how to use this functionality
to prefer servers running on ``localhost``.


.. note::

   Directing queries to servers running on ``localhost`` can be desirable
   when using a sharded cluster with multiple ``mongos``, as locally run
   queries are likely to see lower latency and higher throughput.


.. testsetup::

   from pymongo import MongoClient


.. _server selection algorithm: https://docs.mongodb.com/manual/core/read-preference-mechanics/


Example: Selecting Servers Running on ``localhost``
---------------------------------------------------

To start, we need to write the server selector function that will be used.
The server selector function should accept a list of
:class:`~pymongo.server_description.ServerDescription` objects and return a
list of server descriptions that are suitable for the read or write operation.

In addition to comparing the hostname with ``localhost``, our server selector
function accounts for the following edge cases:

* The list of server descriptions passed to the server selector is empty. This
  can happen if the server selection process starts before the client has
  received successful heartbeats from any of the ``mongod`` or ``mongos``.

* No servers are running on ``localhost``. In this case, we allow the default
  server selection logic to prevail by passing through all server descriptions
  received by the server selector. Failure to do so would render the client
  unable to communicate with MongoDB in the event that no servers were running
  on ``localhost``.


The described server selection logic is implemented in the following server
selctor function:


.. doctest::

   >>> def server_selector(server_descriptions):
   ...     servers = [
   ...         server for server in server_descriptions
   ...         if server.address[0] == 'localhost'
   ...     ]
   ...     if not servers:
   ...         return server_descriptions
   ...     return servers



Finally, we can create a :class:`~pymongo.MongoClient` instance with this
server selector. Optionally, we can also specify a custom value for the
``localThresholdMS`` option which is also used by the
`server selection algorithm`_.


.. doctest::

   >>> client = MongoClient(serverSelector=server_selector)


.. note::

   The user-defined server selector function is applied to host pool *after*
   accounting for the :class:`~pymongo.read_preferences.ReadPreference`,
   but *before* applying the latency window specified by ``localThresholdMS``.


.. _server selection algorithm: https://docs.mongodb.com/manual/core/read-preference-mechanics/