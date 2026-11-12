:mod:`read_preferences` -- Utilities for choosing which member of a replica set to read from.
=============================================================================================

.. automodule:: pymongo.read_preferences
   :synopsis: Utilities for choosing which member of a replica set to read from.

   .. autoclass:: pymongo.read_preferences.Primary

      .. max_staleness, min_wire_version, mongos_mode, and tag_sets don't
         make sense for Primary.

      .. autoattribute:: document
      .. autoattribute:: mode
      .. autoattribute:: name

   .. autoclass:: pymongo.read_preferences.PrimaryPreferred
      :inherited-members:
   .. autoclass:: pymongo.read_preferences.Secondary
      :inherited-members:
   .. autoclass:: pymongo.read_preferences.SecondaryPreferred
      :inherited-members:
   .. autoclass:: pymongo.read_preferences.Nearest
      :inherited-members:

   .. autoclass:: ReadPreference

      .. autoattribute:: PRIMARY
      .. autoattribute:: PRIMARY_PREFERRED
      .. autoattribute:: SECONDARY
      .. autoattribute:: SECONDARY_PREFERRED
      .. autoattribute:: NEAREST
