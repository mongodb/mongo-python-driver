Tailable Cursors
================

By default, MongoDB will automatically close a cursor when the client has
exhausted all results in the cursor. However, for `capped collections
<https://docs.mongodb.org/manual/core/capped-collections/>`_ you may
use a `tailable cursor
<https://docs.mongodb.org/manual/reference/glossary/#term-tailable-cursor>`_
that remains open after the client exhausts the results in the initial cursor.

The following is a basic example of using a tailable cursor to tail the oplog
of a replica set member::

  import time

  import pymongo

  client = pymongo.MongoClient()
  oplog = client.local.oplog.rs
  first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
  print(first)
  ts = first['ts']

  while True:
      # For a regular capped collection CursorType.TAILABLE_AWAIT is the
      # only option required to create a tailable cursor. When querying the
      # oplog the oplog_replay option enables an optimization to quickly
      # find the 'ts' value we're looking for. The oplog_replay option
      # can only be used when querying the oplog.
      cursor = oplog.find({'ts': {'$gt': ts}},
                          cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                          oplog_replay=True)
      while cursor.alive:
          for doc in cursor:
              ts = doc['ts']
              print(doc)
          # We end up here if the find() returned no documents or if the
          # tailable cursor timed out (no new documents were added to the
          # collection for more than 1 second).
          time.sleep(1)
