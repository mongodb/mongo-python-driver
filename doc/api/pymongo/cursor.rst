:mod:`cursor` -- Tools for iterating over MongoDB query results
===============================================================

.. automodule:: pymongo.cursor
   :synopsis: Tools for iterating over MongoDB query results

   .. autoclass:: pymongo.cursor.Cursor(collection, spec=None, fields=None, skip=0, limit=0, timeout=True, snapshot=False, tailable=False, sort=None, max_scan=None, as_class=None, await_data=False, partial=False, manipulate=True, read_preference=ReadPreference.PRIMARY, tag_sets=[{}], secondary_acceptable_latency_ms=None, exhaust=False)
      :members:

      .. describe:: c[index]

         See :meth:`__getitem__`.

      .. automethod:: __getitem__
