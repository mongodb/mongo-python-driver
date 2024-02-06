Logging
========

Starting in 4.8, **PyMongo** supports `Python's native logging library <https://docs.python.org/3/howto/logging.html>`_,
enabling developers to customize the verbosity of log messages for their applications.

Components
-------------
There are currently three different **PyMongo** components with logging support: ``pymongo.command``, ``pymongo.connection``, and ``pymongo.serverSelection``.
These components deal with command operations, connection management, and server selection, respectively.
Each can be configured separately or all together.

Configuration
-------------
