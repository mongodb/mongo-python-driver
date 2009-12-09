:mod:`objectid` -- Tools for working with MongoDB ObjectIds
===========================================================

.. automodule:: pymongo.objectid
   :synopsis: Tools for working with MongoDB ObjectIds

   .. autoclass:: pymongo.objectid.ObjectId([oid=None])
      :members:

      .. describe:: str(o)

         Get a hex encoded version of :class:`ObjectId` `o`.

         The following property always holds:

         .. testsetup::

           from pymongo.objectid import ObjectId

         .. doctest::

           >>> o = ObjectId()
           >>> o == ObjectId(str(o))
           True

         This representation is useful for urls or other places where
         ``o.binary`` is inappropriate.
