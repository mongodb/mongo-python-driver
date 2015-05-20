:mod:`cursor` -- Tools for iterating over MongoDB query results
===============================================================

.. automodule:: pymongo.cursor
   :synopsis: Tools for iterating over MongoDB query results

   .. autoclass:: pymongo.cursor.CursorType

      .. autoattribute:: NON_TAILABLE
         :annotation:
      .. autoattribute:: TAILABLE
         :annotation:
      .. autoattribute:: TAILABLE_AWAIT
         :annotation:
      .. autoattribute:: EXHAUST
         :annotation:

   .. autoclass:: pymongo.cursor.Cursor(collection, spec=None, fields=None, skip=0, limit=0, timeout=True, snapshot=False, tailable=False, sort=None, max_scan=None, as_class=None, slave_okay=False, await_data=False, partial=False, manipulate=True, read_preference=ReadPreference.PRIMARY, tag_sets=[{}], secondary_acceptable_latency_ms=None, exhaust=False, compile_re=True, oplog_replay=False, modifiers=None, network_timeout=None, filter=None, projection=None, no_cursor_timeout=None, allow_partial_results=None, cursor_type=None)
      :members:

      .. describe:: c[index]

         See :meth:`__getitem__`.

      .. automethod:: __getitem__
