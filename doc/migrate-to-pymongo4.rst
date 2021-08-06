PyMongo 4 Migration Guide
=========================

.. contents::

.. testsetup::

  from pymongo import MongoClient, ReadPreference
  client = MongoClient()
  database = client.my_database
  collection = database.my_collection

PyMongo 4.0 brings a number of improvements as well as some backward breaking
changes. This guide provides a roadmap for migrating an existing application
from PyMongo 3.x to 4.x or writing libraries that will work with both
PyMongo 3.x and 4.x.

PyMongo 3
---------

The first step in any successful migration involves upgrading to, or
requiring, at least that latest version of PyMongo 3.x. If your project has a
requirements.txt file, add the line "pymongo >= 3.12, < 4.0" until you have
completely migrated to PyMongo 4. Most of the key new methods and options from
PyMongo 4.0 are backported in PyMongo 3.12 making migration much easier.

.. note:: Users of PyMongo 2.X who wish to upgrade to 4.x must first upgrade
   to PyMongo 3.x by following the :doc:`migrate-to-pymongo3`.

Python 3.6+
-----------

PyMongo 4.0 drops support for Python 2.7, 3.4, and 3.5. Users who wish to
upgrade to 4.x must first upgrade to Python 3.6+. Users upgrading from
Python 2 should consult the :doc:`python3`.

Enable Deprecation Warnings
---------------------------

:exc:`DeprecationWarning` is raised by most methods removed in PyMongo 4.0.
Make sure you enable runtime warnings to see where deprecated functions and
methods are being used in your application::

  python -Wd <your application>

Warnings can also be changed to errors::

  python -Wd -Werror <your application>

.. note:: Not all deprecated features raise :exc:`DeprecationWarning` when
  used. See `Removed features with no migration path`_.

MongoReplicaSetClient
---------------------

Removed :class:`~pymongo.mongo_replica_set_client.MongoReplicaSetClient`.
Since PyMongo 3.0, ``MongoReplicaSetClient`` has been identical to
:class:`pymongo.mongo_client.MongoClient`. Applications can simply replace
``MongoReplicaSetClient`` with :class:`pymongo.mongo_client.MongoClient` and
get the same behavior.

MongoClient
-----------

MongoClient.fsync is removed
............................

Removed :meth:`pymongo.mongo_client.MongoClient.fsync`. Run the
`fsync command`_ directly with :meth:`~pymongo.database.Database.command`
instead. For example::

    client.admin.command('fsync', lock=True)

.. _fsync command: https://docs.mongodb.com/manual/reference/command/fsync/

MongoClient.unlock is removed
.............................

Removed :meth:`pymongo.mongo_client.MongoClient.unlock`. Users of MongoDB
version 3.2 or newer can run the `fsyncUnlock command`_ directly with
:meth:`~pymongo.database.Database.command`::

     client.admin.command('fsyncUnlock')

Users of MongoDB version 2.6 and 3.0 can query the "unlock" virtual
collection::

    client.admin["$cmd.sys.unlock"].find_one()

.. _fsyncUnlock command: https://docs.mongodb.com/manual/reference/command/fsyncUnlock/

MongoClient.is_locked is removed
................................

Removed :attr:`pymongo.mongo_client.MongoClient.is_locked`. Users of MongoDB
version 3.2 or newer can run the `currentOp command`_ directly with
:meth:`~pymongo.database.Database.command`::

    is_locked = client.admin.command('currentOp').get('fsyncLock')

Users of MongoDB version 2.6 and 3.0 can query the "inprog" virtual
collection::

    is_locked = client.admin["$cmd.sys.inprog"].find_one().get('fsyncLock')

.. _currentOp command: https://docs.mongodb.com/manual/reference/command/currentOp/

MongoClient.database_names is removed
.....................................

Removed :meth:`pymongo.mongo_client.MongoClient.database_names`. Use
:meth:`~pymongo.mongo_client.MongoClient.list_database_names` instead. Code like
this::

    names = client.database_names()

can be changed to this::

    names = client.list_database_names()

Database
--------

Database.authenticate and Database.logout are removed
.....................................................

Removed :meth:`pymongo.database.Database.authenticate` and
:meth:`pymongo.database.Database.logout`. Authenticating multiple users
on the same client conflicts with support for logical sessions in MongoDB 3.6+.
To authenticate as multiple users, create multiple instances of
:class:`~pymongo.mongo_client.MongoClient`. Code like this::

    client = MongoClient()
    client.admin.authenticate('user1', 'pass1')
    client.admin.authenticate('user2', 'pass2')

can be changed to this::

    client1 = MongoClient(username='user1', password='pass1')
    client2 = MongoClient(username='user2', password='pass2')

Alternatively, create a single user that contains all the authentication privileges
required by your application.

Database.collection_names is removed
....................................

Removed :meth:`pymongo.database.Database.collection_names`. Use
:meth:`~pymongo.database.Database.list_collection_names` instead. Code like
this::

    names = client.collection_names()
    non_system_names = client.collection_names(include_system_collections=False)

can be changed to this::

    names = client.list_collection_names()
    non_system_names = client.list_collection_names(filter={"name": {"$regex": r"^(?!system\\.)"}})

Database.current_op is removed
..............................

Removed :meth:`pymongo.database.Database.current_op`. Use
:meth:`~pymongo.database.Database.aggregate` instead with the
`$currentOp aggregation pipeline stage`_. Code like
this::

    ops = client.admin.current_op()['inprog']

can be changed to this::

    ops = list(client.admin.aggregate([{'$currentOp': {}}]))

.. _$currentOp aggregation pipeline stage: https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/

Database.add_user is removed
............................

Removed :meth:`pymongo.database.Database.add_user`  which was deprecated in
PyMongo 3.6. Use the `createUser command`_ or `updateUser command`_ instead.
To create a user::

  db.command("createUser", "admin", pwd="password", roles=["dbAdmin"])

To create a read-only user::

  db.command("createUser", "user", pwd="password", roles=["read"])

To change a password::

  db.command("updateUser", "user", pwd="newpassword")

Or change roles::

  db.command("updateUser", "user", roles=["readWrite"])

.. _createUser command: https://docs.mongodb.com/manual/reference/command/createUser/
.. _updateUser command: https://docs.mongodb.com/manual/reference/command/updateUser/

Database.remove_user is removed
...............................

Removed :meth:`pymongo.database.Database.remove_user` which was deprecated in
PyMongo 3.6. Use the `dropUser command`_ instead::

  db.command("dropUser", "user")

.. _dropUser command: https://docs.mongodb.com/manual/reference/command/createUser/

Database.profiling_level is removed
...................................

Removed :meth:`pymongo.database.Database.profiling_level` which was deprecated in
PyMongo 3.12. Use the `profile command`_ instead. Code like this::

  level = db.profiling_level()

Can be changed to this::

  profile = db.command('profile', -1)
  level = profile['was']

.. _profile command: https://docs.mongodb.com/manual/reference/command/profile/

Database.set_profiling_level is removed
.......................................

Removed :meth:`pymongo.database.Database.set_profiling_level` which was deprecated in
PyMongo 3.12. Use the `profile command`_ instead. Code like this::

  db.set_profiling_level(pymongo.ALL, filter={'op': 'query'})

Can be changed to this::

  res = db.command('profile', 2, filter={'op': 'query'})

Database.profiling_info is removed
..................................

Removed :meth:`pymongo.database.Database.profiling_info` which was deprecated in
PyMongo 3.12. Query the `'system.profile' collection`_ instead. Code like this::

  profiling_info = db.profiling_info()

Can be changed to this::

  profiling_info = list(db['system.profile'].find())

.. _'system.profile' collection: https://docs.mongodb.com/manual/reference/database-profiler/

Collection
----------

The useCursor option for Collection.aggregate is removed
........................................................

Removed the ``useCursor`` option for
:meth:`~pymongo.collection.Collection.aggregate` which was deprecated in
PyMongo 3.6. The option was only necessary when upgrading from MongoDB 2.4
to MongoDB 2.6.

Collection.insert is removed
............................

Removed :meth:`pymongo.collection.Collection.insert`. Use
:meth:`~pymongo.collection.Collection.insert_one` or
:meth:`~pymongo.collection.Collection.insert_many` instead.

Code like this::

  collection.insert({'doc': 1})
  collection.insert([{'doc': 2}, {'doc': 3}])

Can be changed to this::

  collection.insert_one({'my': 'document'})
  collection.insert_many([{'doc': 2}, {'doc': 3}])

Collection.save is removed
..........................

Removed :meth:`pymongo.collection.Collection.save`. Applications will
get better performance using :meth:`~pymongo.collection.Collection.insert_one`
to insert a new document and :meth:`~pymongo.collection.Collection.update_one`
to update an existing document. Code like this::

  doc = collection.find_one({"_id": "some id"})
  doc["some field"] = <some value>
  db.collection.save(doc)

Can be changed to this::

  result = collection.update_one({"_id": "some id"}, {"$set": {"some field": <some value>}})

If performance is not a concern and refactoring is untenable, ``save`` can be
implemented like so::

  def save(doc):
      if '_id' in doc:
          collection.replace_one({'_id': doc['_id']}, doc, upsert=True)
          return doc['_id']
      else:
          res = collection.insert_one(doc)
          return res.inserted_id

Collection.update is removed
............................

Removed :meth:`pymongo.collection.Collection.update`. Use
:meth:`~pymongo.collection.Collection.update_one`
to update a single document or
:meth:`~pymongo.collection.Collection.update_many` to update multiple
documents. Code like this::

  collection.update({}, {'$set': {'a': 1}})
  collection.update({}, {'$set': {'b': 1}}, multi=True)

Can be changed to this::

  collection.update_one({}, {'$set': {'a': 1}})
  collection.update_many({}, {'$set': {'b': 1}})

Collection.remove is removed
............................

Removed :meth:`pymongo.collection.Collection.remove`. Use
:meth:`~pymongo.collection.Collection.delete_one`
to delete a single document or
:meth:`~pymongo.collection.Collection.delete_many` to delete multiple
documents. Code like this::

  collection.remove({'a': 1}, multi=False)
  collection.remove({'b': 1})

Can be changed to this::

  collection.delete_one({'a': 1})
  collection.delete_many({'b': 1})

Collection.find_and_modify is removed
.....................................

Removed :meth:`pymongo.collection.Collection.find_and_modify`. Use
:meth:`~pymongo.collection.Collection.find_one_and_update`,
:meth:`~pymongo.collection.Collection.find_one_and_replace`, or
:meth:`~pymongo.collection.Collection.find_one_and_delete` instead.
Code like this::

  updated_doc = collection.find_and_modify({'a': 1}, {'$set': {'b': 1}})
  replaced_doc = collection.find_and_modify({'b': 1}, {'c': 1})
  deleted_doc = collection.find_and_modify({'c': 1}, remove=True)

Can be changed to this::

  updated_doc = collection.find_one_and_update({'a': 1}, {'$set': {'b': 1}})
  replaced_doc = collection.find_one_and_replace({'b': 1}, {'c': 1})
  deleted_doc = collection.find_one_and_delete({'c': 1})

Collection.initialize_ordered_bulk_op and initialize_unordered_bulk_op is removed
.................................................................................

Removed :meth:`pymongo.collection.Collection.initialize_ordered_bulk_op`
and :class:`pymongo.bulk.BulkOperationBuilder`. Use
:meth:`pymongo.collection.Collection.bulk_write` instead. Code like this::

  batch = coll.initialize_ordered_bulk_op()
  batch.insert({'a': 1})
  batch.find({'a': 1}).update_one({'$set': {'b': 1}})
  batch.find({'a': 2}).upsert().replace_one({'b': 2})
  batch.find({'a': 3}).remove()
  result = batch.execute()

Can be changed to this::

  coll.bulk_write([
      InsertOne({'a': 1}),
      UpdateOne({'a': 1}, {'$set': {'b': 1}}),
      ReplaceOne({'a': 2}, {'b': 2}, upsert=True),
      DeleteOne({'a': 3}),
  ])

Collection.initialize_unordered_bulk_op is removed
..................................................

Removed :meth:`pymongo.collection.Collection.initialize_unordered_bulk_op`.
Use :meth:`pymongo.collection.Collection.bulk_write` instead. Code like this::

  batch = coll.initialize_unordered_bulk_op()
  batch.insert({'a': 1})
  batch.find({'a': 1}).update_one({'$set': {'b': 1}})
  batch.find({'a': 2}).upsert().replace_one({'b': 2})
  batch.find({'a': 3}).remove()
  result = batch.execute()

Can be changed to this::

  coll.bulk_write([
      InsertOne({'a': 1}),
      UpdateOne({'a': 1}, {'$set': {'b': 1}}),
      ReplaceOne({'a': 2}, {'b': 2}, upsert=True),
      DeleteOne({'a': 3}),
  ], ordered=False)

Collection.group is removed
...........................

Removed :meth:`pymongo.collection.Collection.group`. This method was
deprecated in PyMongo 3.5. MongoDB 4.2 removed the `group command`_.
Use :meth:`~pymongo.collection.Collection.aggregate` with the ``$group`` stage
instead.

.. _group command: https://docs.mongodb.com/manual/reference/command/group/

Collection.ensure_index is removed
..................................

Removed :meth:`pymongo.collection.Collection.ensure_index`. Use
:meth:`~pymongo.collection.Collection.create_index` or
:meth:`~pymongo.collection.Collection.create_indexes` instead. Note that
``ensure_index`` maintained an in memory cache of recently created indexes
whereas the newer methods do not. Applications should avoid frequent calls
to :meth:`~pymongo.collection.Collection.create_index` or
:meth:`~pymongo.collection.Collection.create_indexes`. Code like this::

  def persist(self, document):
      collection.ensure_index('a', unique=True)
      collection.insert_one(document)

Can be changed to this::

  def persist(self, document):
      if not self.created_index:
          collection.create_index('a', unique=True)
          self.created_index = True
      collection.insert_one(document)

Collection.reindex is removed
.............................

Removed :meth:`pymongo.collection.Collection.reindex`. Run the
`reIndex command`_ directly instead. Code like this::

  >>> result = database.my_collection.reindex()

can be changed to this::

  >>> result = database.command('reIndex', 'my_collection')

.. _reIndex command: https://docs.mongodb.com/manual/reference/command/reIndex/

SONManipulator is removed
-------------------------

Removed :mod:`pymongo.son_manipulator`,
:class:`pymongo.son_manipulator.SONManipulator`,
:class:`pymongo.son_manipulator.ObjectIdInjector`,
:class:`pymongo.son_manipulator.ObjectIdShuffler`,
:class:`pymongo.son_manipulator.AutoReference`,
:class:`pymongo.son_manipulator.NamespaceInjector`,
:meth:`pymongo.database.Database.add_son_manipulator`,
:attr:`pymongo.database.Database.outgoing_copying_manipulators`,
:attr:`pymongo.database.Database.outgoing_manipulators`,
:attr:`pymongo.database.Database.incoming_copying_manipulators`, and
:attr:`pymongo.database.Database.incoming_manipulators`.

Removed the ``manipulate`` parameter from
:meth:`~pymongo.collection.Collection.find`,
:meth:`~pymongo.collection.Collection.find_one`, and
:meth:`~pymongo.cursor.Cursor`.

The :class:`pymongo.son_manipulator.SONManipulator` API has limitations as a
technique for transforming your data and was deprecated in PyMongo 3.0.
Instead, it is more flexible and straightforward to transform outgoing
documents in your own code before passing them to PyMongo, and transform
incoming documents after receiving them from PyMongo.

Alternatively, if your application uses the ``SONManipulator`` API to convert
custom types to BSON, the :class:`~bson.codec_options.TypeCodec` and
:class:`~bson.codec_options.TypeRegistry` APIs may be a suitable alternative.
For more information, see the
:doc:`custom type example <examples/custom_type>`.

IsMaster is removed
-------------------

Removed :class:`pymongo.ismaster.IsMaster`.
Use :class:`pymongo.hello.Hello` instead.

NotMasterError is removed
-------------------------

Removed :exc:`~pymongo.errors.NotMasterError`.
Use :exc:`~pymongo.errors.NotPrimaryError` instead.

Removed features with no migration path
---------------------------------------

cursor_manager support is removed
.................................

Removed :class:`pymongo.cursor_manager.CursorManager`,
:mod:`pymongo.cursor_manager`, and
:meth:`pymongo.mongo_client.MongoClient.set_cursor_manager`.

MongoClient.close_cursor is removed
...................................

Removed :meth:`pymongo.mongo_client.MongoClient.close_cursor` and
:meth:`pymongo.mongo_client.MongoClient.kill_cursors`. Instead, close cursors
with :meth:`pymongo.cursor.Cursor.close` or
:meth:`pymongo.command_cursor.CommandCursor.close`.

.. _killCursors command: https://docs.mongodb.com/manual/reference/command/killCursors/

Database.eval, Database.system_js, and SystemJS are removed
...........................................................

Removed :meth:`~pymongo.database.Database.eval`,
:data:`~pymongo.database.Database.system_js` and
:class:`~pymongo.database.SystemJS`. The eval command was deprecated in
MongoDB 3.0 and removed in MongoDB 4.2. There is no replacement for eval with
MongoDB 4.2+.

However, on MongoDB <= 4.0, code like this::

  >>> result = database.eval('function (x) {return x;}', 3)

can be changed to this::

  >>> from bson.code import Code
  >>> result = database.command('eval', Code('function (x) {return x;}'), args=[3]).get('retval')

Database.error, Database.last_status, Database.previous_error, and Database.reset_error_history are removed
...........................................................................................................

Removed :meth:`pymongo.database.Database.error`,
:meth:`pymongo.database.Database.last_status`,
:meth:`pymongo.database.Database.previous_error`, and
:meth:`pymongo.database.Database.reset_error_history`.
These methods are obsolete: all MongoDB write operations use an acknowledged
write concern and report their errors by default. These methods were
deprecated in PyMongo 2.8.

Collection.parallel_scan is removed
...................................

Removed :meth:`~pymongo.collection.Collection.parallel_scan`. MongoDB 4.2
removed the `parallelCollectionScan command`_.  There is no replacement.

.. _parallelCollectionScan command: https://docs.mongodb.com/manual/reference/command/parallelCollectionScan/
