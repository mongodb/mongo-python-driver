Server Selector Example
=======================

Users can exert fine-grained control over the `server selection algorithm`_
by setting the `serverSelector` option on the :class:`~pymongo.MongoClient`
to an appropriate callable. This example shows how to use this functionality
to prefer servers running on ``localhost``.


.. warning::

   Use of custom server selector functions is a power user feature. Misusing
   custom server selectors can have unintended consequences such as degraded
   read/write performance.

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
A server selector must not create or modify
:class:`~pymongo.server_description.ServerDescription` objects, and must return
the selected instances unchanged.

In this example, we write a server selector that prioritizes servers running on
``localhost``. This can be desirable when using a sharded cluster with multiple
``mongos``, as locally run queries are likely to see lower latency and higher
throughput. Please note, however, that it is highly dependent on the
application if preferring ``localhost`` is beneficial or not.

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
selector function:


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
server selector.


.. doctest::

   >>> client = MongoClient(serverSelector=server_selector)



Server Selection Process
------------------------

This section dives deeper into the server selection process for reads and
writes. Let's first consider the case of **writes**. Assuming it is connected to a
reachable database, the driver performs the following operations (in order)
during the selection process:

* Select all writeable servers from the list of known hosts. For a replica set
  this is the primary, while for a sharded cluster this can be an arbitrary
  number of ``mongos``

* Apply the user-defined server selector function.

* Apply the ``localThresholdMS`` setting to the list of remaining hosts. This
  whittles the host list down to only contain servers whose latency is at most
  ``localThresholdMS`` milliseconds higher than the lowest observed latency.

* Select a server at random from the remaining host list. The desired
  operation is then performed against the selected server.


For the case of **reads** the process is similar:

* Select all servers matching the user's
  :class:`~pymongo.read_preferences.ReadPreference` from the list of all known
  hosts. For a 3-member replica set with a read preference of
  :class:`~pymongo.read_preferences.Secondary`, this step would select all
  available secondaries.

* Apply the user-defined server selector function.

* Apply the ``localThresholdMS`` setting to the list of remaining hosts. This
  whittles the host list down to only contain servers whose latency is at most
  ``localThresholdMS`` milliseconds higher than the lowest observed latency.

* Select a server at random from the remaining host list. The desired
  operation is then performed against the selected server.


In both cases, the user-defined server selector function is applied
to host pool *after* accounting for the
:class:`~pymongo.read_preferences.ReadPreference`, but *before* applying the
latency window specified by ``localThresholdMS``.

.. _server selection algorithm: https://docs.mongodb.com/manual/core/read-preference-mechanics/