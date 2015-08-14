:mod:`monitoring` -- Tools for monitoring driver events.
========================================================

.. automodule:: pymongo.monitoring
   :synopsis: Tools for monitoring driver events.

   .. data:: COMMAND

      The event type of user commands.
   .. autofunction:: subscribe(subscriber, events=COMMAND)
   .. autofunction:: get_subscribers(event=COMMAND)
   .. autoclass:: Subscriber
      :members:
   .. autoclass:: CommandStartedEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandSucceededEvent
      :members:
      :inherited-members:
   .. autoclass:: CommandFailedEvent
      :members:
      :inherited-members:
