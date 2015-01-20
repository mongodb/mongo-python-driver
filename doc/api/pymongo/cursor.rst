:mod:`cursor` -- Tools for iterating over MongoDB query results
===============================================================

.. automodule:: pymongo.cursor
   :synopsis: Tools for iterating over MongoDB query results

   .. autoclass:: pymongo.cursor.Cursor(collection, filter=None, projection=None, skip=0, limit=0, no_cursor_timeout=False, cursor_type=NON_TAILABLE, sort=None, allow_partial_results=False, oplog_replay=False, modifiers=None, manipulate=True)
      :members:

      .. describe:: c[index]

         See :meth:`__getitem__`.

      .. automethod:: __getitem__
