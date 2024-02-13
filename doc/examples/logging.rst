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

Example
-------------
Here's a simple example that enables ``pymongo.command`` debug logs and performs two database operations::

    import logging
    import pymongo

    # Automatically writes all logs to stdout
    logging.basicConfig()
    logging.getLogger('pymongo.command').setLevel(logging.DEBUG)

    client = pymongo.MongoClient()
    client.db.test.insert_one({"x": 1})
    client.db.test.find_one({"x": 1})
    ---------------------------------
    DEBUG:pymongo.command:{"clientId": {"$oid": "65cbe82614be1fc2beb4e4a9"}, "message": "Command started", "command": "{\"insert\": \"test\", \"ordered\": true, \"lsid\": {\"id\": {\"$binary\": {\"base64\": \"GI7ubVhPSsWd7+OwHEFx6Q==\", \"subType\": \"04\"}}}, \"$db\": \"db\", \"documents\": [{\"x\": 1, \"_id\": {\"$oid\": \"65cbe82614be1fc2beb4e4aa\"}}]}", "commandName": "insert", "databaseName": "db", "requestId": 1144108930, "operationId": 1144108930, "driverConnectionId": 1, "serverConnectionId": 3554, "serverHost": "localhost", "serverPort": 27017}
    DEBUG:pymongo.command:{"clientId": {"$oid": "65cbe82614be1fc2beb4e4a9"}, "message": "Command succeeded", "durationMS": 0.515, "reply": "{\"n\": 1, \"ok\": 1.0}", "commandName": "insert", "databaseName": "db", "requestId": 1144108930, "operationId": 1144108930, "driverConnectionId": 1, "serverConnectionId": 3554, "serverHost": "localhost", "serverPort": 27017}
    DEBUG:pymongo.command:{"clientId": {"$oid": "65cbe82614be1fc2beb4e4a9"}, "message": "Command started", "command": "{\"find\": \"test\", \"filter\": {\"x\": 1}, \"limit\": 1, \"singleBatch\": true, \"lsid\": {\"id\": {\"$binary\": {\"base64\": \"GI7ubVhPSsWd7+OwHEFx6Q==\", \"subType\": \"04\"}}}, \"$db\": \"db\"}", "commandName": "find", "databaseName": "db", "requestId": 470211272, "operationId": 470211272, "driverConnectionId": 1, "serverConnectionId": 3554, "serverHost": "localhost", "serverPort": 27017}
    DEBUG:pymongo.command:{"clientId": {"$oid": "65cbe82614be1fc2beb4e4a9"}, "message": "Command succeeded", "durationMS": 0.621, "reply": "{\"cursor\": {\"firstBatch\": [{\"_id\": {\"$oid\": \"65cbdf391a957ed280001417\"}, \"x\": 1}], \"ns\": \"db.test\"}, \"ok\": 1.0}", "commandName": "find", "databaseName": "db", "requestId": 470211272, "operationId": 470211272, "driverConnectionId": 1, "serverConnectionId": 3554, "serverHost": "localhost", "serverPort": 27017}
