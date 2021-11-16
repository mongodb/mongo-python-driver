#  -*- coding: utf-8 -*-
# Copyright 2015 MongoDB, Inc.
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

"""Simulate a MongoDB server, for use in unittests."""

from __future__ import print_function

__author__ = 'A. Jesse Jiryu Davis'
__email__ = 'jesse@mongodb.com'
__version__ = '1.9.0.dev1'

import atexit
import contextlib
import datetime
import errno
import functools
import inspect
import operator
import os
import random
import select
import ssl as _ssl
import socket
import struct
import traceback
import threading
import time
import weakref
import sys
from codecs import utf_8_decode as _utf_8_decode
from collections import OrderedDict

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

try:
    from collections.abc import Mapping
except:
    from collections import Mapping

try:
    from io import StringIO
except ImportError:
    from cStringIO import StringIO

try:
    from urllib.parse import quote_plus
except ImportError:
    # Python 2
    from urllib import quote_plus

import bson
from bson import codec_options, json_util

CODEC_OPTIONS = codec_options.CodecOptions(document_class=OrderedDict)

PY3 = sys.version_info[0] == 3
if PY3:
    string_type = str
    text_type = str


    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)
else:
    string_type = basestring
    text_type = unicode

    # "raise x, y, z" raises SyntaxError in Python 3.
    exec ("""def reraise(exctype, value, trace=None):
    raise exctype, str(value), trace
""")

__all__ = [
    'MockupDB', 'go', 'going', 'Future', 'wait_until', 'interactive_server',

    'OP_REPLY', 'OP_UPDATE', 'OP_INSERT', 'OP_QUERY', 'OP_GET_MORE',
    'OP_DELETE', 'OP_KILL_CURSORS', 'OP_MSG',

    'QUERY_FLAGS', 'UPDATE_FLAGS', 'INSERT_FLAGS', 'DELETE_FLAGS',
    'REPLY_FLAGS', 'OP_MSG_FLAGS',

    'Request', 'Command', 'OpQuery', 'OpGetMore', 'OpKillCursors', 'OpInsert',
    'OpUpdate', 'OpDelete', 'OpReply', 'OpMsg',

    'Matcher', 'absent',
]


def go(fn, *args, **kwargs):
    """Launch an operation on a thread and get a handle to its future result.

    >>> from time import sleep
    >>> def print_sleep_print(duration):
    ...     sleep(duration)
    ...     print('hello from background thread')
    ...     sleep(duration)
    ...     print('goodbye from background thread')
    ...     return 'return value'
    ...
    >>> future = go(print_sleep_print, 0.1)
    >>> sleep(0.15)
    hello from background thread
    >>> print('main thread')
    main thread
    >>> result = future()
    goodbye from background thread
    >>> result
    'return value'
    """
    if not callable(fn):
        raise TypeError('go() requires a function, not %r' % (fn,))
    result = [None]
    error = []

    def target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception:
            # Are we in interpreter shutdown?
            if sys:
                error.extend(sys.exc_info())

    t = threading.Thread(target=target)
    t.daemon = True
    t.start()

    def get_result(timeout=10):
        t.join(timeout)
        if t.is_alive():
            raise AssertionError('timed out waiting for %r' % fn)
        if error:
            reraise(*error)
        return result[0]

    return get_result


@contextlib.contextmanager
def going(fn, *args, **kwargs):
    """Launch a thread and wait for its result before exiting the code block.

    >>> with going(lambda: 'return value') as future:
    ...    pass
    >>> future()  # Won't block, the future is ready by now.
    'return value'

    Or discard the result:

    >>> with going(lambda: "don't care"):
    ...    pass


    If an exception is raised within the context, the result is lost:

    >>> with going(lambda: 'return value') as future:
    ...    assert 1 == 0
    Traceback (most recent call last):
    ...
    AssertionError
    """
    future = go(fn, *args, **kwargs)
    try:
        yield future
    except:
        # We are raising an exception, just try to clean up the future.
        exc_info = sys.exc_info()
        try:
            # Shorter than normal timeout.
            future(timeout=1)
        except:
            log_message = ('\nerror in %s:\n'
                           % format_call(inspect.currentframe()))
            sys.stderr.write(log_message)
            traceback.print_exc()
            # sys.stderr.write('exc in %s' % format_call(inspect.currentframe()))
        reraise(*exc_info)
    else:
        # Raise exception or discard result.
        future(timeout=10)


class Future(object):
    def __init__(self):
        self._result = None
        self._event = threading.Event()

    def result(self, timeout=None):
        if not self._event.wait(timeout):
            raise AssertionError('timed out waiting for Future')
        return self._result

    def set_result(self, result):
        if self._event.is_set():
            raise RuntimeError("Future is already resolved")
        self._result = result
        self._event.set()


def wait_until(predicate, success_description, timeout=10):
    """Wait up to 10 seconds (by default) for predicate to be true.

    E.g.:

        wait_until(lambda: client.primary == ('a', 1),
                   'connect to the primary')

    If the lambda-expression isn't true after 10 seconds, we raise
    AssertionError("Didn't ever connect to the primary").

    Returns the predicate's first true value.
    """
    start = time.time()
    while True:
        retval = predicate()
        if retval:
            return retval

        if time.time() - start > timeout:
            raise AssertionError("Didn't ever %s" % success_description)

        time.sleep(0.1)


OP_REPLY = 1
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GET_MORE = 2005
OP_DELETE = 2006
OP_KILL_CURSORS = 2007
OP_MSG = 2013

QUERY_FLAGS = OrderedDict([
    ('TailableCursor', 2),
    ('SlaveOkay', 4),
    ('OplogReplay', 8),
    ('NoTimeout', 16),
    ('AwaitData', 32),
    ('Exhaust', 64),
    ('Partial', 128)])

UPDATE_FLAGS = OrderedDict([
    ('Upsert', 1),
    ('MultiUpdate', 2)])

INSERT_FLAGS = OrderedDict([
    ('ContinueOnError', 1)])

DELETE_FLAGS = OrderedDict([
    ('SingleRemove', 1)])

REPLY_FLAGS = OrderedDict([
    ('CursorNotFound', 1),
    ('QueryFailure', 2)])

OP_MSG_FLAGS = OrderedDict([
    ('checksumPresent', 1 << 0),
    ('moreToCome', 1 << 1),
    ('exhaustAllowed', 1 << 16)])

_ALL_OP_MSG_FLAGS = functools.reduce(operator.or_, OP_MSG_FLAGS.values())

_UNPACK_BYTE = struct.Struct("<b").unpack
_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_UINT = struct.Struct("<I").unpack
_UNPACK_LONG = struct.Struct("<q").unpack


def _get_c_string(data, position):
    """Decode a BSON 'C' string to python unicode string."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(data[position:end], None, True)[0], end + 1


class _PeekableQueue(Queue):
    """Only safe from one consumer thread at a time."""
    _NO_ITEM = object()

    def __init__(self, *args, **kwargs):
        Queue.__init__(self, *args, **kwargs)
        self._item = _PeekableQueue._NO_ITEM

    def peek(self, block=True, timeout=None):
        if self._item is not _PeekableQueue._NO_ITEM:
            return self._item
        else:
            self._item = self.get(block, timeout)
            return self._item

    def get(self, block=True, timeout=None):
        if self._item is not _PeekableQueue._NO_ITEM:
            item = self._item
            self._item = _PeekableQueue._NO_ITEM
            return item
        else:
            return Queue.get(self, block, timeout)


def _ismap(obj):
    return isinstance(obj, Mapping)


def _islist(obj):
    return isinstance(obj, list)


def _dt_rounded(dt):
    """Python datetimes have microsecond precision, BSON only milliseconds."""
    return dt.replace(microsecond=dt.microsecond - dt.microsecond % 1000)


class Request(object):
    """Base class for `Command`, `OpMsg`, and so on.

    Some useful asserts you can do in tests:

    >>> {'_id': 0} in OpInsert({'_id': 0})
    True
    >>> {'_id': 1} in OpInsert({'_id': 0})
    False
    >>> {'_id': 1} in OpInsert([{'_id': 0}, {'_id': 1}])
    True
    >>> {'_id': 1} == OpInsert([{'_id': 0}, {'_id': 1}])[1]
    True
    >>> 'field' in OpMsg(field=1)
    True
    >>> 'field' in OpMsg()
    False
    >>> 'field' in OpMsg('ismaster')
    False
    >>> OpMsg(ismaster=False)['ismaster'] is False
    True
    """
    opcode = None
    is_command = None
    _non_matched_attrs = 'doc', 'docs'
    _flags_map = None

    def __init__(self, *args, **kwargs):
        self._flags = kwargs.pop('flags', None)
        self._namespace = kwargs.pop('namespace', None)
        self._client = kwargs.pop('_client', None)
        self._request_id = kwargs.pop('request_id', None)
        self._server = kwargs.pop('_server', None)
        self._verbose = self._server and self._server.verbose
        self._server_port = kwargs.pop('server_port', None)
        self._docs = make_docs(*args, **kwargs)
        if not all(_ismap(doc) for doc in self._docs):
            raise_args_err()

    @property
    def doc(self):
        """The request document, if there is exactly one.

        Use this for queries, commands, and legacy deletes. Legacy writes may
        have many documents, OP_GET_MORE and OP_KILL_CURSORS have none.
        """
        assert len(self.docs) == 1, '%r has more than one document' % self
        return self.docs[0]

    @property
    def docs(self):
        """The request documents, if any."""
        return self._docs

    @property
    def namespace(self):
        """The operation namespace or None."""
        return self._namespace

    @property
    def flags(self):
        """The request flags or None."""
        return self._flags

    @property
    def slave_ok(self):
        """True if the SlaveOkay wire protocol flag is set."""
        return self._flags and bool(
            self._flags & QUERY_FLAGS['SlaveOkay'])

    slave_okay = slave_ok
    """Synonym for `.slave_ok`."""

    @property
    def request_id(self):
        """The request id or None."""
        return self._request_id

    @property
    def client_port(self):
        """Client connection's TCP port."""
        address = self._client.getpeername()
        if isinstance(address, tuple):
            return address[1]

        # Maybe a Unix domain socket connection.
        return 0

    @property
    def server(self):
        """The `.MockupDB` server."""
        return self._server

    def assert_matches(self, *args, **kwargs):
        """Assert this matches a :ref:`message spec <message spec>`.

        Returns self.
        """
        matcher = make_matcher(*args, **kwargs)
        if not matcher.matches(self):
            raise AssertionError('%r does not match %r' % (self, matcher))
        return self

    def matches(self, *args, **kwargs):
        """True if this matches a :ref:`message spec <message spec>`."""
        return make_matcher(*args, **kwargs).matches(self)

    def replies(self, *args, **kwargs):
        """Send an `OpReply` to the client.

        The default reply to a command is ``{'ok': 1}``, otherwise the default
        is empty (no documents).

        Returns True so it is suitable as an `~MockupDB.autoresponds` handler.
        """
        self._replies(*args, **kwargs)
        return True

    ok = send = sends = reply = replies
    """Synonym for `.replies`."""

    def fail(self, err='MockupDB query failure', *args, **kwargs):
        """Reply to a query with the QueryFailure flag and an '$err' key.

        Returns True so it is suitable as an `~MockupDB.autoresponds` handler.
        """
        kwargs.setdefault('flags', 0)
        kwargs['flags'] |= REPLY_FLAGS['QueryFailure']
        kwargs['$err'] = err
        self.replies(*args, **kwargs)
        return True

    def command_err(self, code=1, errmsg='MockupDB command failure',
                    *args, **kwargs):
        """Error reply to a command.

        Returns True so it is suitable as an `~MockupDB.autoresponds` handler.
        """
        kwargs.setdefault('ok', 0)
        kwargs['code'] = code
        kwargs['errmsg'] = errmsg
        self.replies(*args, **kwargs)
        return True

    def hangup(self):
        """Close the connection.

        Returns True so it is suitable as an `~MockupDB.autoresponds` handler.
        """
        if self._server:
            self._server._log('\t%d\thangup' % self.client_port)
        self._client.shutdown(socket.SHUT_RDWR)
        return True

    hangs_up = hangup
    """Synonym for `.hangup`."""

    def _matches_docs(self, docs, other_docs):
        """Overridable method."""
        for doc, other_doc in zip(docs, other_docs):
            if not self._match_map(doc, other_doc):
                return False

        return True

    def _match_map(self, doc, other_doc):
        for key, val in doc.items():
            if val is absent:
                if key in other_doc:
                    return False
            elif not self._match_val(val, other_doc.get(key, None)):
                return False

        if isinstance(doc, (OrderedDict, bson.SON)):
            if not isinstance(other_doc, (OrderedDict, bson.SON)):
                raise TypeError(
                    "Can't compare ordered and unordered document types:"
                    " %r, %r" % (doc, other_doc))
            keys = [key for key, val in doc.items()
                    if val is not absent]
            if not seq_match(keys, list(other_doc.keys())):
                return False

        return True

    def _match_list(self, lst, other_lst):
        if len(lst) != len(other_lst):
            return False

        for val, other_val in zip(lst, other_lst):
            if not self._match_val(val, other_val):
                return False

        return True

    def _match_val(self, val, other_val):
        if _ismap(val) and _ismap(other_val):
            if not self._match_map(val, other_val):
                return False
        elif _islist(val) and _islist(other_val):
            if not self._match_list(val, other_val):
                return False
        elif (isinstance(val, datetime.datetime)
              and isinstance(other_val, datetime.datetime)):
            if _dt_rounded(val) != _dt_rounded(other_val):
                return False
        elif val != other_val:
            return False

        return True

    def _replies(self, *args, **kwargs):
        """Overridable method."""
        reply_msg = make_reply(*args, **kwargs)
        if self._server:
            self._server._log('\t%d\t<-- %r' % (self.client_port, reply_msg))
        reply_bytes = reply_msg.reply_bytes(self)
        self._client.sendall(reply_bytes)

    def __contains__(self, item):
        if item in self.docs:
            return True
        if len(self.docs) == 1 and isinstance(item, (string_type, text_type)):
            return item in self.doc
        return False

    def __getitem__(self, item):
        return self.doc[item] if len(self.docs) == 1 else self.docs[item]

    def __str__(self):
        return docs_repr(*self.docs)

    def __repr__(self):
        name = self.__class__.__name__
        parts = []
        if self.docs:
            parts.append(docs_repr(*self.docs))

        if self._flags:
            if self._flags_map:
                parts.append('flags=%s' % (
                    '|'.join(name for name, value in self._flags_map.items()
                             if self._flags & value)))
            else:
                parts.append('flags=%d' % self._flags)

        if self._namespace:
            parts.append('namespace="%s"' % self._namespace)

        return '%s(%s)' % (name, ', '.join(str(part) for part in parts))


class CommandBase(Request):
    """A command the client executes on the server."""
    is_command = True

    # Check command name case-insensitively.
    _non_matched_attrs = Request._non_matched_attrs + ('command_name',)

    @property
    def command_name(self):
        """The command name or None.

        >>> OpMsg({'count': 'collection'}).command_name
        'count'
        >>> OpMsg('aggregate', 'collection', cursor=absent).command_name
        'aggregate'
        """
        if self.docs and self.docs[0]:
            return list(self.docs[0])[0]

    def _matches_docs(self, docs, other_docs):
        assert len(docs) == len(other_docs) == 1
        doc, = docs
        other_doc, = other_docs
        items = list(doc.items())
        other_items = list(other_doc.items())

        # Compare command name case-insensitively.
        if items and other_items:
            if items[0][0].lower() != other_items[0][0].lower():
                return False
            if items[0][1] != other_items[0][1]:
                return False
        return super(CommandBase, self)._matches_docs(
            [OrderedDict(items[1:])],
            [OrderedDict(other_items[1:])])


class OpMsg(CommandBase):
    """An OP_MSG request the client executes on the server."""
    opcode = OP_MSG
    is_command = True
    _flags_map = OP_MSG_FLAGS

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpMsg`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        payload_document = OrderedDict()
        flags, = _UNPACK_UINT(msg[:4])
        pos = 4
        if flags & ~_ALL_OP_MSG_FLAGS:
            raise ValueError(
                'OP_MSG flags has reserved bits set.'
                ' Allowed flags: 0x%x. Provided flags: 0x%x' % (
                    _ALL_OP_MSG_FLAGS, flags))

        checksum_present = flags & OP_MSG_FLAGS['checksumPresent']
        checksum = None
        if checksum_present:
            msg_len_without_checksum = len(msg) - 4
        else:
            msg_len_without_checksum = len(msg)

        while pos < msg_len_without_checksum:
            payload_type, = _UNPACK_BYTE(msg[pos:pos + 1])
            pos += 1
            payload_size, = _UNPACK_INT(msg[pos:pos + 4])
            if payload_type == 0:
                doc = bson.decode_all(msg[pos:pos + payload_size],
                                      CODEC_OPTIONS)[0]
                payload_document.update(doc)
                pos += payload_size
            elif payload_type == 1:
                section_size, = _UNPACK_INT(msg[pos:pos + 4])
                pos += 4
                identifier, pos = _get_c_string(msg, pos)
                # Section starts w/ 4-byte size prefix, identifier ends w/ nil.
                documents_len = section_size - len(identifier) - 1 - 4
                documents = bson.decode_all(msg[pos:pos + documents_len],
                                            CODEC_OPTIONS)
                payload_document[identifier] = documents
                pos += documents_len

        remaining = len(msg) - pos
        if checksum_present:
            if remaining != 4:
                raise ValueError(
                    'OP_MSG has checksumPresent flag set, expected 4 bytes'
                    ' remaining but have %d bytes remaining' % (remaining,))

            checksum = _UNPACK_UINT(msg[pos:pos+4])[0]
        else:
            if remaining != 0:
                raise ValueError(
                    'OP_MSG has no checksumPresent flag, expected 0 bytes'
                    ' remaining but have %d bytes remaining' % (remaining,))

        database = payload_document['$db']
        return OpMsg(payload_document, namespace=database, flags=flags,
                     _client=client, request_id=request_id, checksum=checksum,
                     _server=server)

    def __init__(self, *args, **kwargs):
        checksum = kwargs.pop('checksum', None)
        super(OpMsg, self).__init__(*args, **kwargs)
        self._checksum = checksum
        if len(self._docs) > 1:
            raise_args_err('OpMsg too many documents', ValueError)

    @property
    def slave_ok(self):
        """True if this OpMsg can read from a secondary."""
        read_preference = self.doc.get('$readPreference')
        return read_preference and read_preference.get('mode') != 'primary'

    @property
    def checksum(self):
        """The provided checksum, if set, else None."""
        return self._checksum

    slave_okay = slave_ok
    """Synonym for `.slave_ok`."""

    @property
    def command_name(self):
        """The command name or None.

        >>> OpMsg({'count': 'collection'}).command_name
        'count'
        >>> OpMsg('aggregate', 'collection', cursor=absent).command_name
        'aggregate'
        """
        if self.docs and self.docs[0]:
            return list(self.docs[0])[0]

    def _replies(self, *args, **kwargs):
        if self.flags & OP_MSG_FLAGS['moreToCome']:
            assert False, "Cannot reply to OpMsg with moreToCome: %r" % (self,)
        reply = make_op_msg_reply(*args, **kwargs)
        if not reply.docs:
            reply.docs = [{'ok': 1}]
        else:
            if len(reply.docs) > 1:
                raise ValueError('OP_MSG reply with multiple documents: %s'
                                 % (reply.docs,))
            reply.doc.setdefault('ok', 1)
        super(OpMsg, self)._replies(reply)


class OpQuery(Request):
    """A query (besides a command) the client executes on the server.

    >>> OpQuery({'i': {'$gt': 2}}, fields={'j': False})
    OpQuery({"i": {"$gt": 2}}, fields={"j": false})
    """
    opcode = OP_QUERY
    is_command = False
    _flags_map = QUERY_FLAGS

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpQuery` or `Command`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        is_command = namespace.endswith('.$cmd')
        num_to_skip, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        num_to_return, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        docs = bson.decode_all(msg[pos:], CODEC_OPTIONS)
        if is_command:
            assert len(docs) == 1
            command_ns = namespace[:-len('.$cmd')]
            return Command(docs, namespace=command_ns, flags=flags,
                           _client=client, request_id=request_id,
                           _server=server)
        else:
            if len(docs) == 1:
                fields = None
            else:
                assert len(docs) == 2
                fields = docs[1]
            return OpQuery(docs[0], fields=fields, namespace=namespace,
                           flags=flags, num_to_skip=num_to_skip,
                           num_to_return=num_to_return, _client=client,
                           request_id=request_id, _server=server)

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        if fields is not None and not _ismap(fields):
            raise_args_err()
        self._fields = fields
        self._num_to_skip = kwargs.pop('num_to_skip', None)
        self._num_to_return = kwargs.pop('num_to_return', None)
        super(OpQuery, self).__init__(*args, **kwargs)
        if not self._docs:
            self._docs = [{}]  # Default query filter.
        elif len(self._docs) > 1:
            raise_args_err('OpQuery too many documents', ValueError)

    @property
    def num_to_skip(self):
        """Client query's numToSkip or None."""
        return self._num_to_skip

    @property
    def num_to_return(self):
        """Client query's numToReturn or None."""
        return self._num_to_return

    @property
    def fields(self):
        """Client query's fields selector or None."""
        return self._fields

    def __repr__(self):
        rep = super(OpQuery, self).__repr__().rstrip(')')
        if self._fields:
            rep += ', fields=%s' % docs_repr(self._fields)
        if self._num_to_skip is not None:
            rep += ', numToSkip=%d' % self._num_to_skip
        if self._num_to_return is not None:
            rep += ', numToReturn=%d' % self._num_to_return
        return rep + ')'


class Command(CommandBase, OpQuery):
    """A command the client executes on the server."""

    def _replies(self, *args, **kwargs):
        reply = make_reply(*args, **kwargs)
        if not reply.docs:
            reply.docs = [{'ok': 1}]
        else:
            if len(reply.docs) > 1:
                raise ValueError('Command reply with multiple documents: %s'
                                 % (reply.docs,))
            reply.doc.setdefault('ok', 1)
        super(Command, self)._replies(reply)

    def replies_to_gle(self, **kwargs):
        """Send a getlasterror response.

        Defaults to ``{ok: 1, err: null}``. Add or override values by passing
        keyword arguments.

        Returns True so it is suitable as an `~MockupDB.autoresponds` handler.
        """
        kwargs.setdefault('err', None)
        return self.replies(**kwargs)


class OpGetMore(Request):
    """An OP_GET_MORE the client executes on the server."""

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpGetMore`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        num_to_return, = _UNPACK_INT(msg[pos:pos + 4])
        pos += 4
        cursor_id, = _UNPACK_LONG(msg[pos:pos + 8])
        return OpGetMore(namespace=namespace, flags=flags, _client=client,
                         num_to_return=num_to_return, cursor_id=cursor_id,
                         request_id=request_id, _server=server)

    def __init__(self, **kwargs):
        self._num_to_return = kwargs.pop('num_to_return', None)
        self._cursor_id = kwargs.pop('cursor_id', None)
        super(OpGetMore, self).__init__(**kwargs)

    @property
    def num_to_return(self):
        """The client message's numToReturn field."""
        return self._num_to_return

    @property
    def cursor_id(self):
        """The client message's cursorId field."""
        return self._cursor_id


class OpKillCursors(Request):
    """An OP_KILL_CURSORS the client executes on the server."""

    @classmethod
    def unpack(cls, msg, client, server, _):
        """Parse message and return an `OpKillCursors`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        # Leading 4 bytes are reserved.
        num_of_cursor_ids, = _UNPACK_INT(msg[4:8])
        cursor_ids = []
        pos = 8
        for _ in range(num_of_cursor_ids):
            cursor_ids.append(_UNPACK_INT(msg[pos:pos + 4])[0])
            pos += 4
        return OpKillCursors(_client=client, cursor_ids=cursor_ids,
                             _server=server)

    def __init__(self, **kwargs):
        self._cursor_ids = kwargs.pop('cursor_ids', None)
        super(OpKillCursors, self).__init__(**kwargs)

    @property
    def cursor_ids(self):
        """List of cursor ids the client wants to kill."""
        return self._cursor_ids

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._cursor_ids)


class _LegacyWrite(Request):
    is_command = False


class OpInsert(_LegacyWrite):
    """A legacy OP_INSERT the client executes on the server."""
    opcode = OP_INSERT
    _flags_map = INSERT_FLAGS

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpInsert`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        flags, = _UNPACK_INT(msg[:4])
        namespace, pos = _get_c_string(msg, 4)
        docs = bson.decode_all(msg[pos:], CODEC_OPTIONS)
        return cls(*docs, namespace=namespace, flags=flags, _client=client,
                   request_id=request_id, _server=server)


class OpUpdate(_LegacyWrite):
    """A legacy OP_UPDATE the client executes on the server."""
    opcode = OP_UPDATE
    _flags_map = UPDATE_FLAGS

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpUpdate`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        # First 4 bytes of OP_UPDATE are "reserved".
        namespace, pos = _get_c_string(msg, 4)
        flags, = _UNPACK_INT(msg[pos:pos + 4])
        docs = bson.decode_all(msg[pos + 4:], CODEC_OPTIONS)
        return cls(*docs, namespace=namespace, flags=flags, _client=client,
                   request_id=request_id, _server=server)


class OpDelete(_LegacyWrite):
    """A legacy OP_DELETE the client executes on the server."""
    opcode = OP_DELETE
    _flags_map = DELETE_FLAGS

    @classmethod
    def unpack(cls, msg, client, server, request_id):
        """Parse message and return an `OpDelete`.

        Takes the client message as bytes, the client and server socket objects,
        and the client request id.
        """
        # First 4 bytes of OP_DELETE are "reserved".
        namespace, pos = _get_c_string(msg, 4)
        flags, = _UNPACK_INT(msg[pos:pos + 4])
        docs = bson.decode_all(msg[pos + 4:], CODEC_OPTIONS)
        return cls(*docs, namespace=namespace, flags=flags, _client=client,
                   request_id=request_id, _server=server)


class Reply(object):
    """A reply from `MockupDB` to the client."""

    def __init__(self, *args, **kwargs):
        self._flags = kwargs.pop('flags', 0)
        self._docs = make_docs(*args, **kwargs)

    @property
    def doc(self):
        """Contents of reply.

        Useful for replies to commands; replies to other messages may have no
        documents or multiple documents.
        """
        assert len(self._docs) == 1, '%s has more than one document' % self
        return self._docs[0]

    def __str__(self):
        return docs_repr(*self._docs)

    def __repr__(self):
        rep = '%s(%s' % (self.__class__.__name__, self)
        if self._flags:
            rep += ', flags=' + '|'.join(
                name for name, value in REPLY_FLAGS.items()
                if self._flags & value)

        return rep + ')'


class OpReply(Reply):
    """An OP_REPLY reply from `MockupDB` to the client."""

    def __init__(self, *args, **kwargs):
        self._cursor_id = kwargs.pop('cursor_id', 0)
        self._starting_from = kwargs.pop('starting_from', 0)
        super(OpReply, self).__init__(*args, **kwargs)

    @property
    def docs(self):
        """The reply documents, if any."""
        return self._docs

    @docs.setter
    def docs(self, docs):
        self._docs = make_docs(docs)

    def update(self, *args, **kwargs):
        """Update the document. Same as ``dict().update()``.

           >>> reply = OpReply({'ismaster': True})
           >>> reply.update(maxWireVersion=3)
           >>> reply.doc['maxWireVersion']
           3
           >>> reply.update({'maxWriteBatchSize': 10, 'msg': 'isdbgrid'})
        """
        self.doc.update(*args, **kwargs)

    def reply_bytes(self, request):
        """Take a `Request` and return an OP_REPLY message as bytes."""
        flags = struct.pack("<i", self._flags)
        cursor_id = struct.pack("<q", self._cursor_id)
        starting_from = struct.pack("<i", self._starting_from)
        number_returned = struct.pack("<i", len(self._docs))
        reply_id = random.randint(0, 1000000)
        response_to = request.request_id

        data = b''.join([flags, cursor_id, starting_from, number_returned])
        data += b''.join([bson.BSON.encode(doc) for doc in self._docs])

        message = struct.pack("<i", 16 + len(data))
        message += struct.pack("<i", reply_id)
        message += struct.pack("<i", response_to)
        message += struct.pack("<i", OP_REPLY)
        return message + data


class OpMsgReply(Reply):
    """A OP_MSG reply from `MockupDB` to the client."""

    def __init__(self, *args, **kwargs):
        super(OpMsgReply, self).__init__(*args, **kwargs)
        assert len(self._docs) <= 1, 'OpMsgReply can only have one document'

    @property
    def docs(self):
        """The reply documents, if any."""
        return self._docs

    @docs.setter
    def docs(self, docs):
        self._docs = make_docs(docs)
        assert len(self._docs) == 1, 'OpMsgReply must have one document'

    def update(self, *args, **kwargs):
        """Update the document. Same as ``dict().update()``.

           >>> reply = OpMsgReply({'ismaster': True})
           >>> reply.update(maxWireVersion=3)
           >>> reply.doc['maxWireVersion']
           3
           >>> reply.update({'maxWriteBatchSize': 10, 'msg': 'isdbgrid'})
        """
        self.doc.update(*args, **kwargs)

    def reply_bytes(self, request):
        """Take a `Request` and return an OP_MSG message as bytes."""
        flags = struct.pack("<I", self._flags)
        payload_type = struct.pack("<b", 0)
        payload_data = bson.BSON.encode(self.doc)
        data = b''.join([flags, payload_type, payload_data])

        reply_id = random.randint(0, 1000000)
        response_to = request.request_id

        header = struct.pack(
            "<iiii", 16 + len(data), reply_id, response_to, OP_MSG)
        return header + data

    def __repr__(self):
        rep = '%s(%s' % (self.__class__.__name__, self)
        if self._flags:
            rep += ', flags=' + '|'.join(
                name for name, value in OP_MSG_FLAGS.items()
                if self._flags & value)

        return rep + ')'


absent = {'absent': 1}


class Matcher(object):
    """Matches a subset of `.Request` objects.

    Initialized with a :ref:`message spec <message spec>`.

    Used by `~MockupDB.receives` to assert the client sent the expected request,
    and by `~MockupDB.got` to test if it did and return ``True`` or ``False``.
    Used by `.autoresponds` to match requests with autoresponses.
    """

    def __init__(self, *args, **kwargs):
        self._kwargs = kwargs
        self._prototype = make_prototype_request(*args, **kwargs)

    def matches(self, *args, **kwargs):
        """Test if a request matches a :ref:`message spec <message spec>`.

        Returns ``True`` or ``False``.
        """
        request = make_prototype_request(*args, **kwargs)
        if self._prototype.opcode not in (None, request.opcode):
            return False
        if self._prototype.is_command not in (None, request.is_command):
            return False
        for name in dir(self._prototype):
            if name.startswith('_') or name in request._non_matched_attrs:
                # Ignore privates, and handle documents specially.
                continue
            prototype_value = getattr(self._prototype, name, None)
            if inspect.ismethod(prototype_value):
                continue
            actual_value = getattr(request, name, None)
            if prototype_value not in (None, actual_value):
                return False
        if len(self._prototype.docs) not in (0, len(request.docs)):
            return False

        return self._prototype._matches_docs(self._prototype.docs, request.docs)

    @property
    def prototype(self):
        """The prototype `.Request` used to match actual requests with."""
        return self._prototype

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._prototype)


def _synchronized(meth):
    """Call method while holding a lock."""

    @functools.wraps(meth)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return meth(self, *args, **kwargs)

    return wrapper


class _AutoResponder(object):
    def __init__(self, server, matcher, *args, **kwargs):
        self._server = server
        if inspect.isfunction(matcher) or inspect.ismethod(matcher):
            if args or kwargs:
                raise_args_err()
            self._matcher = Matcher()  # Match anything.
            self._handler = matcher
            self._args = ()
            self._kwargs = {}
        else:
            self._matcher = make_matcher(matcher)
            if args and callable(args[0]):
                self._handler = args[0]
                if args[1:] or kwargs:
                    raise_args_err()
                self._args = ()
                self._kwargs = {}
            else:
                self._handler = None
                self._args = args
                self._kwargs = kwargs

    def handle(self, request):
        if self._matcher.matches(request):
            if self._handler:
                return self._handler(request)
            else:
                # Command.replies() overrides Request.replies() with special
                # logic, which is why we saved args and kwargs until now to
                # pass it into request.replies, instead of making an OpReply
                # ourselves in __init__.
                request.replies(*self._args, **self._kwargs)
                return True

    def cancel(self):
        """Stop autoresponding."""
        self._server.cancel_responder(self)

    def __repr__(self):
        return '_AutoResponder(%r, %r, %r)' % (
            self._matcher, self._args, self._kwargs)


_shutting_down = False
_global_threads = weakref.WeakKeyDictionary()


def _shut_down(threads):
    global _shutting_down
    _shutting_down = True

    for t in threads:
        try:
            t.join(10)
        except:
            pass


atexit.register(_shut_down, _global_threads)


class MockupDB(object):
    """A simulated mongod or mongos.

    Call `run` to start the server, and always `close` it to avoid exceptions
    during interpreter shutdown.

    See the tutorial for comprehensive examples.

    :Optional parameters:
      - `port`: listening port number. If not specified, choose
        some unused port and return the port number from `run`.
      - `verbose`: if ``True``, print requests and replies to stdout.
      - `request_timeout`: seconds to wait for the next client request, or else
        assert. Default 10 seconds. Pass int(1e6) to disable.
      - `auto_ismaster`: pass ``True`` to autorespond ``{'ok': 1}`` to
        ismaster requests, or pass a dict or `OpReply`.
      - `ssl`: pass ``True`` to require SSL.
      - `min_wire_version`: the minWireVersion to include in ismaster responses
        if `auto_ismaster` is True, default 0.
      - `max_wire_version`: the maxWireVersion to include in ismaster responses
        if `auto_ismaster` is True, default 6.
      - `uds_path`: a Unix domain socket path. MockupDB will attempt to delete
        the path if it already exists.
    """

    def __init__(self, port=None, verbose=False,
                 request_timeout=10, auto_ismaster=None,
                 ssl=False, min_wire_version=0, max_wire_version=6,
                 uds_path=None):
        if port is not None and uds_path is not None:
            raise TypeError(
                ("You can't pass port=%s and uds_path=%s,"
                 " pass only one or neither") % (port, uds_path))

        self._uds_path = uds_path
        if uds_path:
            self._address = (uds_path, 0)
        else:
            self._address = ('localhost', port)

        self._verbose = verbose
        self._label = None
        self._ssl = ssl

        self._request_timeout = request_timeout

        self._listening_sock = None
        self._accept_thread = None

        # Track sockets that we want to close in stop(). Keys are sockets,
        # values are None (this could be a WeakSet but it's new in Python 2.7).
        self._server_threads = weakref.WeakKeyDictionary()
        self._server_socks = weakref.WeakKeyDictionary()
        self._stopped = False
        self._request_q = _PeekableQueue()
        self._requests_count = 0
        self._lock = threading.Lock()

        # List of (request_matcher, args, kwargs), where args and kwargs are
        # like those sent to request.reply().
        self._autoresponders = []

        if auto_ismaster is True:
            self.autoresponds(CommandBase('ismaster'),
                              {'ismaster': True,
                               'minWireVersion': min_wire_version,
                               'maxWireVersion': max_wire_version})
        elif auto_ismaster:
            self.autoresponds(CommandBase('ismaster'), auto_ismaster)

    @_synchronized
    def run(self):
        """Begin serving. Returns the bound port, or 0 for domain socket."""
        self._listening_sock, self._address = (
            bind_domain_socket(self._address)
            if self._uds_path
            else bind_tcp_socket(self._address))

        if self._ssl:
            certfile = os.path.join(os.path.dirname(__file__), 'server.pem')
            self._listening_sock = _ssl.wrap_socket(
                self._listening_sock,
                certfile=certfile,
                server_side=True)
        self._accept_thread = threading.Thread(target=self._accept_loop)
        self._accept_thread.daemon = True
        self._accept_thread.start()
        return self.port

    @_synchronized
    def stop(self):
        """Stop serving. Always call this to clean up after yourself."""
        self._stopped = True
        threads = [self._accept_thread]
        threads.extend(self._server_threads)
        self._listening_sock.close()
        for sock in list(self._server_socks):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass

            try:
                sock.close()
            except socket.error:
                pass

        with self._unlock():
            for thread in threads:
                thread.join(10)

        if self._uds_path:
            try:
                os.unlink(self._uds_path)
            except OSError:
                pass

    def receives(self, *args, **kwargs):
        """Pop the next `Request` and assert it matches.

        Returns None if the server is stopped.

        Pass a `Request` or request pattern to specify what client request to
        expect. See the tutorial for examples. Pass ``timeout`` as a keyword
        argument to override this server's ``request_timeout``.
        """
        timeout = kwargs.pop('timeout', self._request_timeout)
        end = time.time() + timeout
        matcher = Matcher(*args, **kwargs)
        while not self._stopped:
            try:
                # Short timeout so we notice if the server is stopped.
                request = self._request_q.get(timeout=0.05)
            except Empty:
                if time.time() > end:
                    raise AssertionError('expected to receive %r, got nothing'
                                         % matcher.prototype)
            else:
                if matcher.matches(request):
                    return request
                else:
                    raise AssertionError('expected to receive %r, got %r'
                                         % (matcher.prototype, request))

    gets = pop = receive = receives
    """Synonym for `receives`."""

    def got(self, *args, **kwargs):
        """Does `.request` match the given :ref:`message spec <message spec>`?

        >>> s = MockupDB(auto_ismaster=True)
        >>> port = s.run()
        >>> s.got(timeout=0)  # No request enqueued.
        False
        >>> from pymongo import MongoClient
        >>> client = MongoClient(s.uri)
        >>> future = go(client.db.command, 'foo')
        >>> s.got('foo')
        True
        >>> s.got(OpMsg('foo', namespace='db'))
        True
        >>> s.got(OpMsg('foo', key='value'))
        False
        >>> s.ok()
        >>> future() == {'ok': 1}
        True
        >>> s.stop()
        """
        timeout = kwargs.pop('timeout', self._request_timeout)
        end = time.time() + timeout
        matcher = make_matcher(*args, **kwargs)

        while not self._stopped:
            try:
                # Short timeout so we notice if the server is stopped.
                request = self._request_q.peek(timeout=timeout)
            except Empty:
                if time.time() > end:
                    return False
            else:
                return matcher.matches(request)

    wait = got
    """Synonym for `got`."""

    def replies(self, *args, **kwargs):
        """Call `~Request.reply` on the currently enqueued request."""
        self.pop().replies(*args, **kwargs)

    ok = send = sends = reply = replies
    """Synonym for `.replies`."""

    def fail(self, *args, **kwargs):
        """Call `~Request.fail` on the currently enqueued request."""
        self.pop().fail(*args, **kwargs)

    def command_err(self, *args, **kwargs):
        """Call `~Request.command_err` on the currently enqueued request."""
        self.pop().command_err(*args, **kwargs)

    def hangup(self):
        """Call `~Request.hangup` on the currently enqueued request."""
        self.pop().hangup()

    hangs_up = hangup
    """Synonym for `.hangup`."""

    @_synchronized
    def autoresponds(self, matcher, *args, **kwargs):
        """Send a canned reply to all matching client requests.

        ``matcher`` is a `Matcher` or a command name, or an instance of
        `OpInsert`, `OpQuery`, etc.

        >>> s = MockupDB()
        >>> port = s.run()
        >>>
        >>> from pymongo import MongoClient
        >>> client = MongoClient(s.uri)
        >>> responder = s.autoresponds('ismaster', maxWireVersion=6)
        >>> client.admin.command('ismaster') == {'ok': 1, 'maxWireVersion': 6}
        True

        The remaining arguments are a :ref:`message spec <message spec>`:

        >>> # ok
        >>> responder = s.autoresponds('bar', ok=0, errmsg='err')
        >>> client.db.command('bar')
        Traceback (most recent call last):
        ...
        OperationFailure: command SON([('bar', 1)]) on namespace db.$cmd failed: err
        >>> responder = s.autoresponds(OpMsg('find', 'collection'),
        ...                            {'cursor': {'id': 0, 'firstBatch': [{'_id': 1}, {'_id': 2}]}})
        >>> # ok
        >>> list(client.db.collection.find()) == [{'_id': 1}, {'_id': 2}]
        True
        >>> responder = s.autoresponds(OpMsg('find', 'collection'),
        ...                            {'cursor': {'id': 0, 'firstBatch': [{'a': 1}, {'a': 2}]}})
        >>> # bad
        >>> list(client.db.collection.find()) == [{'a': 1}, {'a': 2}]
        True

        Remove an autoresponder like:

        >>> responder.cancel()

        If the request currently at the head of the queue matches, it is popped
        and replied to. Future matching requests skip the queue.

        >>> future = go(client.db.command, 'baz')
        >>> # bad
        >>> responder = s.autoresponds('baz', {'key': 'value'})
        >>> future() == {'ok': 1, 'key': 'value'}
        True

        Responders are applied in order, most recently added first, until one
        matches:

        >>> responder = s.autoresponds('baz')
        >>> client.db.command('baz') == {'ok': 1}
        True
        >>> responder.cancel()
        >>> # The previous responder takes over again.
        >>> client.db.command('baz') == {'ok': 1, 'key': 'value'}
        True

        You can pass a request handler in place of the message spec. Return
        True if you handled the request:

        >>> responder = s.autoresponds('baz', lambda r: r.ok(a=2))

        The standard `Request.ok`, `~Request.replies`, `~Request.fail`,
        `~Request.hangup` and so on all return True to make them suitable
        as handler functions.

        >>> client.db.command('baz') == {'ok': 1, 'a': 2}
        True

        If the request is not handled, it is checked against the remaining
        responders, or enqueued if none match.

        You can pass the handler as the only argument so it receives *all*
        requests. For example you could log them, then return None to allow
        other handlers to run:

        >>> def logger(request):
        ...     if not request.matches('ismaster'):
        ...         print('logging: %r' % request)
        >>> responder = s.autoresponds(logger)
        >>> client.db.command('baz') == {'ok': 1, 'a': 2}
        logging: OpMsg({"baz": 1, "$db": "db", "$readPreference": {"mode": "primaryPreferred"}}, namespace="db")
        True

        The synonym `subscribe` better expresses your intent if your handler
        never returns True:

        >>> subscriber = s.subscribe(logger)

        .. doctest:
            :hide:

            >>> client.close()
            >>> s.stop()
        """
        return self._insert_responder("top", matcher, *args, **kwargs)

    subscribe = autoresponds
    """Synonym for `.autoresponds`."""

    @_synchronized
    def append_responder(self, matcher, *args, **kwargs):
        """Add a responder of last resort.

        Like `.autoresponds`, but instead of adding a responder to the top of
        the stack, add it to the bottom. This responder will be called if no
        others match.
        """
        return self._insert_responder("bottom", matcher, *args, **kwargs)

    def _insert_responder(self, where, matcher, *args, **kwargs):
        responder = _AutoResponder(self, matcher, *args, **kwargs)
        if where == "top":
            self._autoresponders.append(responder)
        elif where == "bottom":
            self._autoresponders.insert(0, responder)
        else:
            raise RuntimeError("Invalid 'where': %r" % (where,))

        try:
            request = self._request_q.peek(block=False)
        except Empty:
            pass
        else:
            if responder.handle(request):
                self._request_q.get_nowait()  # Pop it.

        return responder

    @_synchronized
    def cancel_responder(self, responder):
        """Cancel a responder that was registered with `autoresponds`."""
        self._autoresponders.remove(responder)

    @property
    def address(self):
        """The listening (host, port)."""
        return self._address

    @property
    def address_string(self):
        """The listening "host:port"."""
        return format_addr(self._address)

    @property
    def host(self):
        """The listening hostname."""
        return self._address[0]

    @property
    def port(self):
        """The listening port."""
        return self._address[1]

    @property
    def uri(self):
        """Connection string to pass to `~pymongo.mongo_client.MongoClient`."""
        if self._uds_path:
            uri = 'mongodb://%s' % (quote_plus(self._uds_path),)
        else:
            uri = 'mongodb://%s' % (format_addr(self._address),)
        return uri + '/?ssl=true' if self._ssl else uri

    @property
    def verbose(self):
        """If verbose logging is turned on."""
        return self._verbose

    @verbose.setter
    def verbose(self, value):
        if not isinstance(value, bool):
            raise TypeError('value must be True or False, not %r' % value)
        self._verbose = value

    @property
    def label(self):
        """Label for logging, or None."""
        return self._label

    @label.setter
    def label(self, value):
        self._label = value

    @property
    def requests_count(self):
        """Number of requests this server has received.

        Includes autoresponded requests.
        """
        return self._requests_count

    @property
    def request(self):
        """The currently enqueued `Request`, or None.

        .. warning:: This property is useful to check what the current request
           is, but the pattern ``server.request.replies()`` is dangerous: you
           must follow it with ``server.pop()`` or the current request remains
           enqueued. Better to reply with ``server.pop().replies()`` than
           ``server.request.replies()`` or any variation on it.
        """
        return self.got() or None

    @property
    @_synchronized
    def running(self):
        """If this server is started and not stopped."""
        return self._accept_thread and not self._stopped

    def _accept_loop(self):
        """Accept client connections and spawn a thread for each."""
        self._listening_sock.setblocking(0)
        while not self._stopped and not _shutting_down:
            try:
                # Wait a short time to accept.
                if select.select([self._listening_sock.fileno()], [], [], 1):
                    client, client_addr = self._listening_sock.accept()
                    client.setblocking(True)
                    self._log('connection from %s' % format_addr(client_addr))
                    server_thread = threading.Thread(
                        target=functools.partial(
                            self._server_loop, client, client_addr))

                    # Store weakrefs to the thread and socket, so we can
                    # dispose them in stop().
                    self._server_threads[server_thread] = None
                    self._server_socks[client] = None

                    server_thread.daemon = True
                    server_thread.start()
            except socket.error as error:
                if error.errno not in (
                        errno.EAGAIN, errno.EBADF, errno.ENOTSOCK,
                        errno.EWOULDBLOCK):
                    raise
            except select.error as error:
                if error.args[0] in (errno.EBADF, errno.ENOTSOCK):
                    # Closed.
                    break
                else:
                    raise

    @_synchronized
    def _server_loop(self, client, client_addr):
        """Read requests from one client socket, 'client'."""
        while not self._stopped and not _shutting_down:
            try:
                with self._unlock():
                    request = mock_server_receive_request(client, self)

                self._requests_count += 1
                self._log('%d\t%r' % (request.client_port, request))

                # Give most recently added responders precedence.
                for responder in reversed(self._autoresponders):
                    if responder.handle(request):
                        self._log('\t(autoresponse)')
                        break
                else:
                    self._request_q.put(request)
            except socket.error as error:
                if error.errno in (errno.ECONNRESET, errno.EBADF,
                                   errno.ENOTSOCK):
                    # We hung up, or the client did.
                    break
                raise
            except select.error as error:
                if error.args[0] in (errno.EBADF, errno.ENOTSOCK):
                    # Closed.
                    break
                else:
                    raise
            except AssertionError:
                traceback.print_exc()
                break

        self._log('disconnected: %s' % format_addr(client_addr))
        client.close()

    def _log(self, msg):
        if self._verbose:
            if self._label:
                msg = '%s:\t%s' % (self._label, msg)
            print(msg)

    @contextlib.contextmanager
    def _unlock(self):
        """Temporarily release the lock."""
        self._lock.release()
        try:
            yield
        finally:
            self._lock.acquire()

    def __iter__(self):
        return self

    def next(self):
        request = self.receives()
        if request is None:
            # Server stopped.
            raise StopIteration()
        return request

    __next__ = next

    def __repr__(self):
        if self._uds_path:
            return 'MockupDB(uds_path=%s)' % (self._uds_path,)

        return 'MockupDB(%s, %s)' % self._address


def format_addr(address):
    """Turn a TCP or Unix domain socket address into a string."""
    if isinstance(address, tuple):
        if address[1]:
            return '%s:%d' % address
        else:
            return address[0]

    return address


def bind_tcp_socket(address):
    """Takes (host, port) and returns (socket_object, (host, port)).

    If the passed-in port is None, bind an unused port and return it.
    """
    host, port = address
    for res in set(socket.getaddrinfo(host, port, socket.AF_INET,
                                      socket.SOCK_STREAM, 0,
                                      socket.AI_PASSIVE)):

        family, socktype, proto, _, sock_addr = res
        sock = socket.socket(family, socktype, proto)
        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Automatic port allocation with port=None.
        sock.bind(sock_addr)
        sock.listen(128)
        bound_port = sock.getsockname()[1]
        return sock, (host, bound_port)

    raise socket.error('could not bind socket')


def bind_domain_socket(address):
    """Takes (socket path, 0) and returns (socket_object, (path, 0))."""
    path, _ = address
    try:
        os.unlink(path)
    except OSError:
        pass

    sock = socket.socket(socket.AF_UNIX)
    sock.bind(path)
    sock.listen(128)
    return sock, (path, 0)


OPCODES = {OP_MSG: OpMsg,
           OP_QUERY: OpQuery,
           OP_INSERT: OpInsert,
           OP_UPDATE: OpUpdate,
           OP_DELETE: OpDelete,
           OP_GET_MORE: OpGetMore,
           OP_KILL_CURSORS: OpKillCursors}


def mock_server_receive_request(client, server):
    """Take a client socket and return a Request."""
    header = mock_server_receive(client, 16)
    length = _UNPACK_INT(header[:4])[0]
    request_id = _UNPACK_INT(header[4:8])[0]
    opcode = _UNPACK_INT(header[12:])[0]
    msg_bytes = mock_server_receive(client, length - 16)
    if opcode not in OPCODES:
        raise NotImplementedError("Don't know how to unpack opcode %d yet"
                                  % opcode)
    return OPCODES[opcode].unpack(msg_bytes, client, server, request_id)


def _errno_from_exception(exc):
    if hasattr(exc, 'errno'):
        return exc.errno
    elif exc.args:
        return exc.args[0]
    else:
        return None


def mock_server_receive(sock, length):
    """Receive `length` bytes from a socket object."""
    msg = b''
    while length:
        chunk = sock.recv(length)
        if chunk == b'':
            raise socket.error(errno.ECONNRESET, 'closed')

        length -= len(chunk)
        msg += chunk

    return msg


def make_docs(*args, **kwargs):
    """Make the documents for a `Request` or `Reply`.

    Takes a variety of argument styles, returns a list of dicts.

    Used by `make_prototype_request` and `make_reply`, which are in turn used by
    `MockupDB.receives`, `Request.replies`, and so on. See examples in
    tutorial.
    """
    err_msg = "Can't interpret args: "
    if not args and not kwargs:
        return []

    if not args:
        # OpReply(ok=1, ismaster=True).
        return [kwargs]

    if isinstance(args[0], (int, float, bool)):
        # server.receives().ok(0, err='uh oh').
        if args[1:]:
            raise_args_err(err_msg, ValueError)
        doc = OrderedDict({'ok': args[0]})
        doc.update(kwargs)
        return [doc]

    if isinstance(args[0], (list, tuple)):
        # Send a batch: OpReply([{'a': 1}, {'a': 2}]).
        if not all(isinstance(doc, (OpReply, Mapping))
                   for doc in args[0]):
            raise_args_err('each doc must be a dict:')
        if kwargs:
            raise_args_err(err_msg, ValueError)
        return list(args[0])

    if isinstance(args[0], (string_type, text_type)):
        if args[2:]:
            raise_args_err(err_msg, ValueError)

        if len(args) == 2:
            # Command('aggregate', 'collection', {'cursor': {'batchSize': 1}}).
            doc = OrderedDict({args[0]: args[1]})
        else:
            # OpReply('ismaster', me='a.com').
            doc = OrderedDict({args[0]: 1})
        doc.update(kwargs)
        return [doc]

    if kwargs:
        raise_args_err(err_msg, ValueError)

    # Send a batch as varargs: OpReply({'a': 1}, {'a': 2}).
    if not all(isinstance(doc, (OpReply, Mapping)) for doc in args):
        raise_args_err('each doc must be a dict')

    return args


def make_matcher(*args, **kwargs):
    """Make a Matcher from a :ref:`message spec <message spec>`:

    >>> make_matcher()
    Matcher(Request())
    >>> make_matcher({'ismaster': 1}, namespace='admin')
    Matcher(Request({"ismaster": 1}, namespace="admin"))
    >>> make_matcher({}, {'_id': 1})
    Matcher(Request({}, {"_id": 1}))

    See more examples in the tutorial section for :ref:`Message Specs`.
    """
    if args and isinstance(args[0], Matcher):
        if args[1:] or kwargs:
            raise_args_err("can't interpret args")
        return args[0]

    return Matcher(*args, **kwargs)


def make_prototype_request(*args, **kwargs):
    """Make a prototype Request for a Matcher."""
    if args and inspect.isclass(args[0]) and issubclass(args[0], Request):
        request_cls, arg_list = args[0], args[1:]
        return request_cls(*arg_list, **kwargs)
    if args and isinstance(args[0], Request):
        if args[1:] or kwargs:
            raise_args_err("can't interpret args")
        return args[0]

    # Match any opcode.
    return Request(*args, **kwargs)


def make_reply(*args, **kwargs):
    # Error we might raise.
    if args and isinstance(args[0], (OpReply, OpMsgReply)):
        if args[1:] or kwargs:
            raise_args_err("can't interpret args")
        return args[0]

    return OpReply(*args, **kwargs)


def make_op_msg_reply(*args, **kwargs):
    # Error we might raise.
    if args and isinstance(args[0], (OpReply, OpMsgReply)):
        if args[1:] or kwargs:
            raise_args_err("can't interpret args")
        return args[0]

    return OpMsgReply(*args, **kwargs)


def unprefixed(bson_str):
    rep = unicode(repr(bson_str))
    if rep.startswith(u'u"') or rep.startswith(u"u'"):
        return rep[1:]
    else:
        return rep


def docs_repr(*args):
    """Stringify ordered dicts like a regular ones.

    Preserve order, remove 'u'-prefix on unicodes in Python 2:

    >>> print(docs_repr(OrderedDict([(u'_id', 2)])))
    {"_id": 2}
    >>> print(docs_repr(OrderedDict([(u'_id', 2), (u'a', u'b')]),
    ...                 OrderedDict([(u'a', 1)])))
    {"_id": 2, "a": "b"}, {"a": 1}
    >>>
    >>> import datetime
    >>> now = datetime.datetime.utcfromtimestamp(123456)
    >>> print(docs_repr(OrderedDict([(u'ts', now)])))
    {"ts": {"$date": 123456000}}
    >>>
    >>> oid = bson.ObjectId(b'123456781234567812345678')
    >>> print(docs_repr(OrderedDict([(u'oid', oid)])))
    {"oid": {"$oid": "123456781234567812345678"}}
    """
    sio = StringIO()
    for doc_idx, doc in enumerate(args):
        if doc_idx > 0:
            sio.write(u', ')
        sio.write(text_type(json_util.dumps(doc)))
    return sio.getvalue()


def seq_match(seq0, seq1):
    """True if seq0 is a subset of seq1 and their elements are in same order.

    >>> seq_match([], [])
    True
    >>> seq_match([1], [1])
    True
    >>> seq_match([1, 1], [1])
    False
    >>> seq_match([1], [1, 2])
    True
    >>> seq_match([1, 1], [1, 1])
    True
    >>> seq_match([3], [1, 2, 3])
    True
    >>> seq_match([1, 3], [1, 2, 3])
    True
    >>> seq_match([2, 1], [1, 2, 3])
    False
    """
    len_seq1 = len(seq1)
    if len_seq1 < len(seq0):
        return False
    seq1_idx = 0
    for i, elem in enumerate(seq0):
        while seq1_idx < len_seq1:
            if seq1[seq1_idx] == elem:
                break
            seq1_idx += 1
        if seq1_idx >= len_seq1 or seq1[seq1_idx] != elem:
            return False
        seq1_idx += 1

    return True


def format_call(frame):
    fn_name = inspect.getframeinfo(frame)[2]
    arg_info = inspect.getargvalues(frame)
    args = [repr(arg_info.locals[arg]) for arg in arg_info.args]
    varargs = [repr(x) for x in arg_info.locals[arg_info.varargs]]
    kwargs = [', '.join("%s=%r" % (key, value) for key, value in
                        arg_info.locals[arg_info.keywords].items())]
    return '%s(%s)' % (fn_name, ', '.join(args + varargs + kwargs))


def raise_args_err(message='bad arguments', error_class=TypeError):
    """Throw an error with standard message, displaying function call.

    >>> def f(a, *args, **kwargs):
    ...     raise_args_err()
    ...
    >>> f(1, 2, x='y')
    Traceback (most recent call last):
    ...
    TypeError: bad arguments: f(1, 2, x='y')
    """
    frame = inspect.currentframe().f_back
    raise error_class(message + ': ' + format_call(frame))


def interactive_server(port=27017, verbose=True, all_ok=False, name='MockupDB',
                       ssl=False, uds_path=None):
    """A `MockupDB` that the mongo shell can connect to.

    Call `~.MockupDB.run` on the returned server, and clean it up with
    `~.MockupDB.stop`.

    If ``all_ok`` is True, replies {ok: 1} to anything unmatched by a specific
    responder.
    """
    if uds_path is not None:
        port = None

    server = MockupDB(port=port,
                      verbose=verbose,
                      request_timeout=int(1e6),
                      ssl=ssl,
                      auto_ismaster=True,
                      uds_path=uds_path)
    if all_ok:
        server.append_responder({})
    server.autoresponds('whatsmyuri', you='localhost:12345')
    server.autoresponds({'getLog': 'startupWarnings'},
                        log=['hello from %s!' % name])
    server.autoresponds(OpMsg('buildInfo'), version='MockupDB ' + __version__)
    server.autoresponds(OpMsg('listCollections'))
    server.autoresponds('replSetGetStatus', ok=0)
    server.autoresponds('getFreeMonitoringStatus', ok=0)
    return server
