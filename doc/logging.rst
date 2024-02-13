Logging
========

Starting in 4.8, **PyMongo** supports `Python's native logging library <https://docs.python.org/3/howto/logging.html>`_,
enabling developers to customize the verbosity of log messages for their applications.

Components
-------------
There are currently three different **PyMongo** components with logging support: ``pymongo.command``, ``pymongo.connection``, and ``pymongo.serverSelection``.
These components deal with command operations, connection management, and server selection, respectively.
Each can be configured separately or they can all be configured together.

Configuration
-------------
Currently, the above components each support ``DEBUG`` logging. To enable a single component, do the following::

    import logging
    logging.getLogger('pymongo.<componentName>').setLevel(logging.DEBUG)



For example, to enable command logging::

    import logging
    logging.getLogger('pymongo.command').setLevel(logging.DEBUG)


You can also enable all ``DEBUG`` logs at once::

    import logging
    logging.getLogger('pymongo').setLevel(logging.DEBUG)


Truncation
-------------
When ``pymongo.command`` debug logs are enabled, every command sent to the server and every response sent back will be included as part of the logs.
By default, these command and response documents are truncated after 1000 bytes.

You can configure a higher truncation limit by setting the ``MONGOB_LOG_MAX_DOCUMENT_LENGTH`` environment variable to your desired length.

Note that by default, only sensitive authentication command contents are redacted.
All commands containing user data will be logged, including the actual contents of your queries.
To prevent this behavior, set ``MONGOB_LOG_MAX_DOCUMENT_LENGTH`` to 0. This will omit the command and response bodies from the logs.
