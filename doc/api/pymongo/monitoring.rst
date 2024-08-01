:mod:`monitoring` -- Tools for monitoring driver events.
========================================================

.. automodule:: pymongo.monitoring
   :synopsis: Tools for monitoring driver events.

   .. autofunction:: register(listener)
   .. autoclass:: CommandListener
      :members:
      :inherited-members:
   .. autoclass:: ServerListener
      :members:
      :inherited-members:
   .. autoclass:: ServerHeartbeatListener
      :members:
      :inherited-members:
   .. autoclass:: TopologyListener
      :members:
      :inherited-members:
   .. autoclass:: ConnectionPoolListener
      :members:
      :inherited-members:

   .. autoclass:: CommandStartedEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandSucceededEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandFailedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerDescriptionChangedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerOpeningEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerClosedEvent
      :members:
      :inherited-members:
   .. autoclass:: TopologyDescriptionChangedEvent
      :members:
      :inherited-members:
   .. autoclass:: TopologyOpenedEvent
      :members:
      :inherited-members:
   .. autoclass:: TopologyClosedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerHeartbeatStartedEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerHeartbeatSucceededEvent
      :members:
      :inherited-members:
   .. autoclass:: ServerHeartbeatFailedEvent
      :members:
      :inherited-members:

   .. autoclass:: PoolCreatedEvent
      :members:
      :inherited-members:
   .. autoclass:: PoolClearedEvent
      :members:
      :inherited-members:
   .. autoclass:: PoolClosedEvent
      :members:
      :inherited-members:

   .. autoclass:: ConnectionCreatedEvent
      :members:
      :inherited-members:
   .. autoclass:: ConnectionReadyEvent
      :members:
      :inherited-members:

   .. autoclass:: ConnectionClosedReason
      :members:

   .. autoclass:: ConnectionClosedEvent
      :members:
      :inherited-members:
   .. autoclass:: ConnectionCheckOutStartedEvent
      :members:
      :inherited-members:

   .. autoclass:: ConnectionCheckOutFailedReason
      :members:

   .. autoclass:: ConnectionCheckOutFailedEvent
      :members:
      :inherited-members:
   .. autoclass:: ConnectionCheckedOutEvent
      :members:
      :inherited-members:
   .. autoclass:: ConnectionCheckedInEvent
      :members:
      :inherited-members:
