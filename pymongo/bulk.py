# Copyright 2014-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The bulk write operations interface.

.. versionadded:: 2.7
"""

from bson.objectid import ObjectId
from bson.son import SON
from pymongo.errors import (BulkWriteError,
                            DocumentTooLarge,
                            InvalidOperation,
                            OperationFailure)
from pymongo.message import (_INSERT, _UPDATE, _DELETE,
                             insert, _do_batched_write_command)


_DELETE_ALL = 0
_DELETE_ONE = 1

# For backwards compatibility. See MongoDB src/mongo/base/error_codes.err
_BAD_VALUE = 2
_UNKNOWN_ERROR = 8
_WRITE_CONCERN_ERROR = 64

_COMMANDS = ('insert', 'update', 'delete')


class _Run(object):
    """Represents a batch of write operations.
    """
    def __init__(self, op_type):
        """Initialize a new Run object.
        """
        self.op_type = op_type
        self.index_map = []
        self.ops = []

    def index(self, idx):
        """Get the original index of an operation in this run.

        :Parameters:
          - `idx`: The Run index that maps to the original index.
        """
        return self.index_map[idx]

    def add(self, original_index, operation):
        """Add an operation to this Run instance.

        :Parameters:
          - `original_index`: The original index of this operation
            within a larger bulk operation.
          - `operation`: The operation document.
        """
        self.index_map.append(original_index)
        self.ops.append(operation)


def _make_error(index, code, errmsg, operation):
    """Create and return an error document.
    """
    return {
        u"index": index,
        u"code": code,
        u"errmsg": errmsg,
        u"op": operation
    }


def _merge_legacy(run, full_result, result, index):
    """Merge a result from a legacy opcode into the full results.
    """
    affected = result.get('n', 0)

    errmsg = result.get("errmsg", result.get("err", ""))
    if errmsg:
        # wtimeout is not considered a hard failure in
        # MongoDB 2.6 so don't treat it like one here.
        if result.get("wtimeout"):
            error_doc = {'errmsg': errmsg, 'code': _WRITE_CONCERN_ERROR}
            full_result['writeConcernErrors'].append(error_doc)
        else:
            code = result.get("code", _UNKNOWN_ERROR)
            error = _make_error(run.index(index), code, errmsg, run.ops[index])
            if "errInfo" in result:
                error["errInfo"] = result["errInfo"]
            full_result["writeErrors"].append(error)
            return

    if run.op_type == _INSERT:
        full_result['nInserted'] += 1

    elif run.op_type == _UPDATE:
        if "upserted" in result:
            doc = {u"index": run.index(index), u"_id": result["upserted"]}
            full_result["upserted"].append(doc)
            full_result['nUpserted'] += affected
        # Versions of MongoDB before 2.6 don't return the _id for an
        # upsert if _id is not an ObjectId.
        elif result.get("updatedExisting") == False and affected == 1:
            op = run.ops[index]
            # If _id is in both the update document *and* the query spec
            # the update document _id takes precedence.
            _id = op['u'].get('_id', op['q'].get('_id'))
            doc = {u"index": run.index(index), u"_id": _id}
            full_result["upserted"].append(doc)
            full_result['nUpserted'] += affected
        else:
            full_result['nMatched'] += affected

    elif run.op_type == _DELETE:
        full_result['nRemoved'] += affected


def _merge_command(run, full_result, results):
    """Merge a group of results from write commands into the full result.
    """
    for offset, result in results:

        affected = result.get("n", 0)

        if run.op_type == _INSERT:
            full_result["nInserted"] += affected

        elif run.op_type == _DELETE:
            full_result["nRemoved"] += affected

        elif run.op_type == _UPDATE:
            upserted = result.get("upserted")
            if upserted:
                if isinstance(upserted, list):
                    n_upserted = len(upserted)
                    for doc in upserted:
                        doc["index"] = run.index(doc["index"] + offset)
                    full_result["upserted"].extend(upserted)
                else:
                    n_upserted = 1
                    index = run.index(offset)
                    doc = {u"index": index, u"_id": upserted}
                    full_result["upserted"].append(doc)
                full_result["nUpserted"] += n_upserted
                full_result["nMatched"] += (affected - n_upserted)
            else:
                full_result["nMatched"] += affected
            n_modified = result.get("nModified")
            # SERVER-13001 - in a mixed sharded cluster a call to
            # update could return nModified (>= 2.6) or not (<= 2.4).
            # If any call does not return nModified we can't report
            # a valid final count so omit the field completely.
            if n_modified is not None and "nModified" in full_result:
                full_result["nModified"] += n_modified
            else:
                full_result.pop("nModified", None)

        write_errors = result.get("writeErrors")
        if write_errors:
            for doc in write_errors:
                idx = doc["index"] + offset
                doc["index"] = run.index(idx)
                # Add the failed operation to the error document.
                doc[u"op"] = run.ops[idx]
            full_result["writeErrors"].extend(write_errors)

        wc_error = result.get("writeConcernError")
        if wc_error:
            full_result["writeConcernErrors"].append(wc_error)


class _Bulk(object):
    """The private guts of the bulk write API.
    """
    def __init__(self, collection, ordered):
        """Initialize a _Bulk instance.
        """
        self.collection = collection
        self.ordered = ordered
        self.ops = []
        self.name = "%s.%s" % (collection.database.name, collection.name)
        self.namespace = collection.database.name + '.$cmd'
        self.executed = False

    def add_insert(self, document):
        """Add an insert document to the list of ops.
        """
        if not isinstance(document, dict):
            raise TypeError('document must be an instance of dict')
        # Generate ObjectId client side.
        if '_id' not in document:
            document['_id'] = ObjectId()
        self.ops.append((_INSERT, document))

    def add_update(self, selector, update, multi=False, upsert=False):
        """Create an update document and add it to the list of ops.
        """
        if not isinstance(update, dict):
            raise TypeError('update must be an instance of dict')
        # Update can not be {}
        if not update:
            raise ValueError('update only works with $ operators')
        first = iter(update).next()
        if not first.startswith('$'):
            raise ValueError('update only works with $ operators')
        cmd = SON([('q', selector), ('u', update),
                   ('multi', multi), ('upsert', upsert)])
        self.ops.append((_UPDATE, cmd))

    def add_replace(self, selector, replacement, upsert=False):
        """Create a replace document and add it to the list of ops.
        """
        if not isinstance(replacement, dict):
            raise TypeError('replacement must be an instance of dict')
        # Replacement can be {}
        if replacement:
            first = iter(replacement).next()
            if first.startswith('$'):
                raise ValueError('replacement can not include $ operators')
        cmd = SON([('q', selector), ('u', replacement),
                   ('multi', False), ('upsert', upsert)])
        self.ops.append((_UPDATE, cmd))

    def add_delete(self, selector, limit):
        """Create a delete document and add it to the list of ops.
        """
        cmd = SON([('q', selector), ('limit', limit)])
        self.ops.append((_DELETE, cmd))

    def gen_ordered(self):
        """Generate batches of operations, batched by type of
        operation, in the order **provided**.
        """
        run = None
        for idx, (op_type, operation) in enumerate(self.ops):
            if run is None:
                run = _Run(op_type)
            elif run.op_type != op_type:
                yield run
                run = _Run(op_type)
            run.add(idx, operation)
        yield run

    def gen_unordered(self):
        """Generate batches of operations, batched by type of
        operation, in arbitrary order.
        """
        operations = [_Run(_INSERT), _Run(_UPDATE), _Run(_DELETE)]
        for idx, (op_type, operation) in enumerate(self.ops):
            operations[op_type].add(idx, operation)

        for run in operations:
            if run.ops:
                yield run

    def execute_command(self, generator, write_concern):
        """Execute using write commands.
        """
        uuid_subtype = self.collection.uuid_subtype
        client = self.collection.database.connection
        # nModified is only reported for write commands, not legacy ops.
        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nRemoved": 0,
            "upserted": [],
        }
        for run in generator:
            cmd = SON([(_COMMANDS[run.op_type], self.collection.name),
                       ('ordered', self.ordered)])
            if write_concern:
                cmd['writeConcern'] = write_concern

            results = _do_batched_write_command(self.namespace,
                run.op_type, cmd, run.ops, True, uuid_subtype, client)

            _merge_command(run, full_result, results)
            # We're supposed to continue if errors are
            # at the write concern level (e.g. wtimeout)
            if self.ordered and full_result['writeErrors']:
                break

        if full_result["writeErrors"] or full_result["writeConcernErrors"]:
            if full_result['writeErrors']:
                full_result['writeErrors'].sort(
                    key=lambda error: error['index'])
            raise BulkWriteError(full_result)
        return full_result

    def execute_no_results(self, generator):
        """Execute all operations, returning no results (w=0).
        """
        coll = self.collection
        w_value = 0
        # If ordered is True we have to send GLE or use write
        # commands so we can abort on the first error.
        if self.ordered:
            w_value = 1

        for run in generator:
            try:
                if run.op_type == _INSERT:
                    coll.insert(run.ops,
                                continue_on_error=not self.ordered,
                                w=w_value)
                else:
                    for operation in run.ops:
                        try:
                            if run.op_type == _UPDATE:
                                coll.update(operation['q'],
                                            operation['u'],
                                            upsert=operation['upsert'],
                                            multi=operation['multi'],
                                            w=w_value)
                            else:
                                coll.remove(operation['q'],
                                            multi=(not operation['limit']),
                                            w=w_value)
                        except OperationFailure:
                            if self.ordered:
                                return
            except OperationFailure:
                if self.ordered:
                    break

    def legacy_insert(self, operation, write_concern):
        """Do a legacy insert and return the result.
        """
        # We have to do this here since Collection.insert
        # throws away results and we need to check for jnote.
        client = self.collection.database.connection
        uuid_subtype = self.collection.uuid_subtype
        return client._send_message(
            insert(self.name, [operation], True, True,
                write_concern, False, uuid_subtype), True)

    def execute_legacy(self, generator, write_concern):
        """Execute using legacy wire protocol ops.
        """
        coll = self.collection
        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nRemoved": 0,
            "upserted": [],
        }
        stop = False
        for run in generator:
            for idx, operation in enumerate(run.ops):
                try:
                    # To do per-operation reporting we have to do ops one
                    # at a time. That means the performance of bulk insert
                    # will be slower here than calling Collection.insert()
                    if run.op_type == _INSERT:
                        result = self.legacy_insert(operation, write_concern)
                    elif run.op_type == _UPDATE:
                        result = coll.update(operation['q'],
                                             operation['u'],
                                             upsert=operation['upsert'],
                                             multi=operation['multi'],
                                             **write_concern)
                    else:
                        result = coll.remove(operation['q'],
                                             multi=(not operation['limit']),
                                             **write_concern)
                    _merge_legacy(run, full_result, result, idx)
                except DocumentTooLarge, exc:
                    # MongoDB 2.6 uses error code 2 for "too large".
                    error = _make_error(
                        run.index(idx), _BAD_VALUE, str(exc), operation)
                    full_result['writeErrors'].append(error)
                    if self.ordered:
                        stop = True
                        break
                except OperationFailure, exc:
                    if not exc.details:
                        # Some error not related to the write operation
                        # (e.g. kerberos failure). Re-raise immediately.
                        raise
                    _merge_legacy(run, full_result, exc.details, idx)
                    # We're supposed to continue if errors are
                    # at the write concern level (e.g. wtimeout)
                    if self.ordered and full_result["writeErrors"]:
                        stop = True
                        break
            if stop:
                break

        if full_result["writeErrors"] or full_result['writeConcernErrors']:
            if full_result['writeErrors']:
                full_result['writeErrors'].sort(
                    key=lambda error: error['index'])
            raise BulkWriteError(full_result)
        return full_result

    def execute(self, write_concern):
        """Execute operations.
        """
        if not self.ops:
            raise InvalidOperation('No operations to execute')
        if self.executed:
            raise InvalidOperation('Bulk operations can '
                                   'only be executed once.')
        self.executed = True
        client = self.collection.database.connection
        client._ensure_connected(sync=True)
        write_concern = write_concern or self.collection.write_concern

        if self.ordered:
            generator = self.gen_ordered()
        else:
            generator = self.gen_unordered()

        if write_concern.get('w') == 0:
            self.execute_no_results(generator)
        elif client.max_wire_version > 1:
            return self.execute_command(generator, write_concern)
        else:
            return self.execute_legacy(generator, write_concern)


class BulkUpsertOperation(object):
    """An interface for adding upsert operations.
    """

    __slots__ = ('__selector', '__bulk')

    def __init__(self, selector, bulk):
        self.__selector = selector
        self.__bulk = bulk

    def update_one(self, update):
        """Update one document matching the selector.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector,
                               update, multi=False, upsert=True)

    def update(self, update):
        """Update all documents matching the selector.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector,
                               update, multi=True, upsert=True)

    def replace_one(self, replacement):
        """Replace one entire document matching the selector criteria.

        :Parameters:
          - `replacement` (dict): the replacement document
        """
        self.__bulk.add_replace(self.__selector, replacement, upsert=True)


class BulkWriteOperation(object):
    """An interface for adding update or remove operations.
    """

    __slots__ = ('__selector', '__bulk')

    def __init__(self, selector, bulk):
        self.__selector = selector
        self.__bulk = bulk

    def update_one(self, update):
        """Update one document matching the selector criteria.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector, update, multi=False)

    def update(self, update):
        """Update all documents matching the selector criteria.

        :Parameters:
          - `update` (dict): the update operations to apply
        """
        self.__bulk.add_update(self.__selector, update, multi=True)

    def replace_one(self, replacement):
        """Replace one entire document matching the selector criteria.

        :Parameters:
          - `replacement` (dict): the replacement document
        """
        self.__bulk.add_replace(self.__selector, replacement)

    def remove_one(self):
        """Remove a single document matching the selector criteria.
        """
        self.__bulk.add_delete(self.__selector, _DELETE_ONE)

    def remove(self):
        """Remove all documents matching the selector criteria.
        """
        self.__bulk.add_delete(self.__selector, _DELETE_ALL)

    def upsert(self):
        """Specify that all chained update operations should be
        upserts.

        :Returns:
          - A :class:`BulkUpsertOperation` instance, used to add
            update operations to this bulk operation.
        """
        return BulkUpsertOperation(self.__selector, self.__bulk)


class BulkOperationBuilder(object):
    """An interface for executing a batch of write operations.
    """

    __slots__ = '__bulk'

    def __init__(self, collection, ordered=True):
        """Initialize a new BulkOperationBuilder instance.

        :Parameters:
          - `collection`: A :class:`~pymongo.collection.Collection` instance.
          - `ordered` (optional): If ``True`` all operations will be executed
            serially, in the order provided, and the entire execution will
            abort on the first error. If ``False`` operations will be executed
            in arbitrary order (possibly in parallel on the server), reporting
            any errors that occurred after attempting all operations. Defaults
            to ``True``.

        .. warning::
          If you are using a version of MongoDB older than 2.6 you will
          get much better bulk insert performance using
          :meth:`~pymongo.collection.Collection.insert`.
        """
        self.__bulk = _Bulk(collection, ordered)

    def find(self, selector):
        """Specify selection criteria for bulk operations.

        :Parameters:
          - `selector` (dict): the selection criteria for update
            and remove operations.

        :Returns:
          - A :class:`BulkWriteOperation` instance, used to add
            update and remove operations to this bulk operation.
        """
        if not isinstance(selector, dict):
            raise TypeError('selector must be an instance of dict')
        return BulkWriteOperation(selector, self.__bulk)

    def insert(self, document):
        """Insert a single document.

        :Parameters:
          - `document` (dict): the document to insert
        """
        self.__bulk.add_insert(document)

    def execute(self, write_concern=None):
        """Execute all provided operations.

        :Parameters:
          - write_concern (optional): the write concern for this bulk
            execution.
        """
        if write_concern and not isinstance(write_concern, dict):
            raise TypeError('write_concern must be an instance of dict')
        return self.__bulk.execute(write_concern)
