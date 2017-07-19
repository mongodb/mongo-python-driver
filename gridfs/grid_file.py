# Copyright 2009-2015 MongoDB, Inc.
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

"""Tools for representing files stored in GridFS."""
import datetime
import math
import os

from hashlib import md5

from bson.son import SON
from bson.binary import Binary
from bson.objectid import ObjectId
from bson.py3compat import text_type, StringIO
from gridfs.errors import CorruptGridFile, FileExists, NoFile
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.errors import (ConfigurationError,
                            DuplicateKeyError,
                            OperationFailure)
from pymongo.read_preferences import ReadPreference

try:
    _SEEK_SET = os.SEEK_SET
    _SEEK_CUR = os.SEEK_CUR
    _SEEK_END = os.SEEK_END
# before 2.5
except AttributeError:
    _SEEK_SET = 0
    _SEEK_CUR = 1
    _SEEK_END = 2

EMPTY = b""
NEWLN = b"\n"

"""Default chunk size, in bytes."""
# Slightly under a power of 2, to work well with server's record allocations.
DEFAULT_CHUNK_SIZE = 255 * 1024

_C_INDEX = SON([("files_id", ASCENDING), ("n", ASCENDING)])
_F_INDEX = SON([("filename", ASCENDING), ("uploadDate", ASCENDING)])


def _grid_in_property(field_name, docstring, read_only=False,
                      closed_only=False):
    """Create a GridIn property."""
    def getter(self):
        if closed_only and not self._closed:
            raise AttributeError("can only get %r on a closed file" %
                                 field_name)
        # Protect against PHP-237
        if field_name == 'length':
            return self._file.get(field_name, 0)
        return self._file.get(field_name, None)

    def setter(self, value):
        if self._closed:
            self._coll.files.update_one({"_id": self._file["_id"]},
                                        {"$set": {field_name: value}})
        self._file[field_name] = value

    if read_only:
        docstring += "\n\nThis attribute is read-only."
    elif closed_only:
        docstring = "%s\n\n%s" % (docstring, "This attribute is read-only and "
                                  "can only be read after :meth:`close` "
                                  "has been called.")

    if not read_only and not closed_only:
        return property(getter, setter, doc=docstring)
    return property(getter, doc=docstring)


def _grid_out_property(field_name, docstring):
    """Create a GridOut property."""
    def getter(self):
        self._ensure_file()

        # Protect against PHP-237
        if field_name == 'length':
            return self._file.get(field_name, 0)
        return self._file.get(field_name, None)

    docstring += "\n\nThis attribute is read-only."
    return property(getter, doc=docstring)


class GridIn(object):
    """Class to write data to GridFS.
    """
    def __init__(self, root_collection, **kwargs):
        """Write a file to GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Raises :class:`TypeError` if `root_collection` is not an
        instance of :class:`~pymongo.collection.Collection`.

        Any of the file level options specified in the `GridFS Spec
        <http://dochub.mongodb.org/core/gridfsspec>`_ may be passed as
        keyword arguments. Any additional keyword arguments will be
        set as additional fields on the file document. Valid keyword
        arguments include:

          - ``"_id"``: unique ID for this file (default:
            :class:`~bson.objectid.ObjectId`) - this ``"_id"`` must
            not have already been used for another file

          - ``"filename"``: human name for the file

          - ``"contentType"`` or ``"content_type"``: valid mime-type
            for the file

          - ``"chunkSize"`` or ``"chunk_size"``: size of each of the
            chunks, in bytes (default: 255 kb)

          - ``"encoding"``: encoding used for this file. In Python 2,
            any :class:`unicode` that is written to the file will be
            converted to a :class:`str`. In Python 3, any :class:`str`
            that is written to the file will be converted to
            :class:`bytes`.

        :Parameters:
          - `root_collection`: root collection to write to
          - `**kwargs` (optional): file level options (see above)

        .. versionchanged:: 3.0
           `root_collection` must use an acknowledged
           :attr:`~pymongo.collection.Collection.write_concern`
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an "
                            "instance of Collection")

        # With w=0, 'filemd5' might run before the final chunks are written.
        if not root_collection.write_concern.acknowledged:
            raise ConfigurationError('root_collection must use '
                                     'acknowledged write_concern')

        # Handle alternative naming
        if "content_type" in kwargs:
            kwargs["contentType"] = kwargs.pop("content_type")
        if "chunk_size" in kwargs:
            kwargs["chunkSize"] = kwargs.pop("chunk_size")

        coll = root_collection.with_options(
            read_preference=ReadPreference.PRIMARY)

        kwargs['md5'] = md5()
        # Defaults
        kwargs["_id"] = kwargs.get("_id", ObjectId())
        kwargs["chunkSize"] = kwargs.get("chunkSize", DEFAULT_CHUNK_SIZE)
        object.__setattr__(self, "_coll", coll)
        object.__setattr__(self, "_chunks", coll.chunks)
        object.__setattr__(self, "_file", kwargs)
        object.__setattr__(self, "_buffer", StringIO())
        object.__setattr__(self, "_position", 0)
        object.__setattr__(self, "_chunk_number", 0)
        object.__setattr__(self, "_closed", False)
        object.__setattr__(self, "_ensured_index", False)

    def __create_index(self, collection, index_key, unique):
        doc = collection.find_one(projection={"_id": 1})
        if doc is None:
            try:
                index_keys =[index_spec['key'] for index_spec in collection.list_indexes()]
            except OperationFailure:
                index_keys = []
            if index_key not in index_keys:
                collection.create_index(index_key.items(), unique=unique)

    def __ensure_indexes(self):
        if not object.__getattribute__(self, "_ensured_index"):
            self.__create_index(self._coll.files, _F_INDEX, False)
            self.__create_index(self._coll.chunks, _C_INDEX, True)
            object.__setattr__(self, "_ensured_index", True)

    def abort(self):
        """Remove all chunks/files that may have been uploaded and close.
        """
        self._coll.chunks.delete_many({"files_id": self._file['_id']})
        self._coll.files.delete_one({"_id": self._file['_id']})
        object.__setattr__(self, "_closed", True)


    @property
    def closed(self):
        """Is this file closed?
        """
        return self._closed

    _id = _grid_in_property("_id", "The ``'_id'`` value for this file.",
                            read_only=True)
    filename = _grid_in_property("filename", "Name of this file.")
    name = _grid_in_property("filename", "Alias for `filename`.")
    content_type = _grid_in_property("contentType", "Mime-type for this file.")
    length = _grid_in_property("length", "Length (in bytes) of this file.",
                               closed_only=True)
    chunk_size = _grid_in_property("chunkSize", "Chunk size for this file.",
                                   read_only=True)
    upload_date = _grid_in_property("uploadDate",
                                    "Date that this file was uploaded.",
                                    closed_only=True)
    md5 = _grid_in_property("md5", "MD5 of the contents of this file "
                            "(generated on the server).",
                            closed_only=True)

    def __getattr__(self, name):
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridIn object has no attribute '%s'" % name)

    def __setattr__(self, name, value):
        # For properties of this instance like _buffer, or descriptors set on
        # the class like filename, use regular __setattr__
        if name in self.__dict__ or name in self.__class__.__dict__:
            object.__setattr__(self, name, value)
        else:
            # All other attributes are part of the document in db.fs.files.
            # Store them to be sent to server on close() or if closed, send
            # them now.
            self._file[name] = value
            if self._closed:
                self._coll.files.update_one({"_id": self._file["_id"]},
                                            {"$set": {name: value}})

    def __flush_data(self, data):
        """Flush `data` to a chunk.
        """
        # Ensure the index, even if there's nothing to write, so
        # the filemd5 command always succeeds.
        self.__ensure_indexes()
        self._file['md5'].update(data)

        if not data:
            return
        assert(len(data) <= self.chunk_size)

        chunk = {"files_id": self._file["_id"],
                 "n": self._chunk_number,
                 "data": Binary(data)}

        try:
            self._chunks.insert_one(chunk)
        except DuplicateKeyError:
            self._raise_file_exists(self._file['_id'])
        self._chunk_number += 1
        self._position += len(data)

    def __flush_buffer(self):
        """Flush the buffer contents out to a chunk.
        """
        self.__flush_data(self._buffer.getvalue())
        self._buffer.close()
        self._buffer = StringIO()

    def __flush(self):
        """Flush the file to the database.
        """
        try:
            self.__flush_buffer()

            self._file['md5'] = self._file["md5"].hexdigest()
            self._file["length"] = self._position
            self._file["uploadDate"] = datetime.datetime.utcnow()

            return self._coll.files.insert_one(self._file)
        except DuplicateKeyError:
            self._raise_file_exists(self._id)

    def _raise_file_exists(self, file_id):
        """Raise a FileExists exception for the given file_id."""
        raise FileExists("file with _id %r already exists" % file_id)

    def close(self):
        """Flush the file and close it.

        A closed file cannot be written any more. Calling
        :meth:`close` more than once is allowed.
        """
        if not self._closed:
            self.__flush()
            object.__setattr__(self, "_closed", True)

    def write(self, data):
        """Write data to the file. There is no return value.

        `data` can be either a string of bytes or a file-like object
        (implementing :meth:`read`). If the file has an
        :attr:`encoding` attribute, `data` can also be a
        :class:`unicode` (:class:`str` in python 3) instance, which
        will be encoded as :attr:`encoding` before being written.

        Due to buffering, the data may not actually be written to the
        database until the :meth:`close` method is called. Raises
        :class:`ValueError` if this file is already closed. Raises
        :class:`TypeError` if `data` is not an instance of
        :class:`str` (:class:`bytes` in python 3), a file-like object,
        or an instance of :class:`unicode` (:class:`str` in python 3).
        Unicode data is only allowed if the file has an :attr:`encoding`
        attribute.

        :Parameters:
          - `data`: string of bytes or file-like object to be written
            to the file
        """
        if self._closed:
            raise ValueError("cannot write to a closed file")

        try:
            # file-like
            read = data.read
        except AttributeError:
            # string
            if not isinstance(data, (text_type, bytes)):
                raise TypeError("can only write strings or file-like objects")
            if isinstance(data, text_type):
                try:
                    data = data.encode(self.encoding)
                except AttributeError:
                    raise TypeError("must specify an encoding for file in "
                                    "order to write %s" % (text_type.__name__,))
            read = StringIO(data).read

        if self._buffer.tell() > 0:
            # Make sure to flush only when _buffer is complete
            space = self.chunk_size - self._buffer.tell()
            if space:
                try:
                    to_write = read(space)
                except:
                    self.abort()
                    raise
                self._buffer.write(to_write)
                if len(to_write) < space:
                    return  # EOF or incomplete
            self.__flush_buffer()
        to_write = read(self.chunk_size)
        while to_write and len(to_write) == self.chunk_size:
            self.__flush_data(to_write)
            to_write = read(self.chunk_size)
        self._buffer.write(to_write)

    def writelines(self, sequence):
        """Write a sequence of strings to the file.

        Does not add seperators.
        """
        for line in sequence:
            self.write(line)

    def __enter__(self):
        """Support for the context manager protocol.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for the context manager protocol.

        Close the file and allow exceptions to propagate.
        """
        self.close()

        # propagate exceptions
        return False


class GridOut(object):
    """Class to read data out of GridFS.
    """
    def __init__(self, root_collection, file_id=None, file_document=None):
        """Read a file from GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Either `file_id` or `file_document` must be specified,
        `file_document` will be given priority if present. Raises
        :class:`TypeError` if `root_collection` is not an instance of
        :class:`~pymongo.collection.Collection`.

        :Parameters:
          - `root_collection`: root collection to read from
          - `file_id` (optional): value of ``"_id"`` for the file to read
          - `file_document` (optional): file document from
            `root_collection.files`

        .. versionchanged:: 3.0
           Creating a GridOut does not immediately retrieve the file metadata
           from the server. Metadata is fetched when first needed.
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an "
                            "instance of Collection")

        self.__chunks = root_collection.chunks
        self.__files = root_collection.files
        self.__file_id = file_id
        self.__buffer = EMPTY
        self.__position = 0
        self._file = file_document

    _id = _grid_out_property("_id", "The ``'_id'`` value for this file.")
    filename = _grid_out_property("filename", "Name of this file.")
    name = _grid_out_property("filename", "Alias for `filename`.")
    content_type = _grid_out_property("contentType", "Mime-type for this file.")
    length = _grid_out_property("length", "Length (in bytes) of this file.")
    chunk_size = _grid_out_property("chunkSize", "Chunk size for this file.")
    upload_date = _grid_out_property("uploadDate",
                                     "Date that this file was first uploaded.")
    aliases = _grid_out_property("aliases", "List of aliases for this file.")
    metadata = _grid_out_property("metadata", "Metadata attached to this file.")
    md5 = _grid_out_property("md5", "MD5 of the contents of this file "
                             "(generated on the server).")

    def _ensure_file(self):
        if not self._file:
            self._file = self.__files.find_one({"_id": self.__file_id})
            if not self._file:
                raise NoFile("no file in gridfs collection %r with _id %r" %
                             (self.__files, self.__file_id))

    def __getattr__(self, name):
        self._ensure_file()
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridOut object has no attribute '%s'" % name)

    def readchunk(self):
        """Reads a chunk at a time. If the current position is within a
        chunk the remainder of the chunk is returned.
        """
        received = len(self.__buffer)
        chunk_data = EMPTY
        chunk_size = int(self.chunk_size)

        if received > 0:
            chunk_data = self.__buffer
        elif self.__position < int(self.length):
            chunk_number = int((received + self.__position) / chunk_size)
            chunk = self.__chunks.find_one({"files_id": self._id,
                                            "n": chunk_number})
            if not chunk:
                raise CorruptGridFile("no chunk #%d" % chunk_number)

            chunk_data = chunk["data"][self.__position % chunk_size:]

            if not chunk_data:
                raise CorruptGridFile("truncated chunk")

        self.__position += len(chunk_data)
        self.__buffer = EMPTY
        return chunk_data

    def read(self, size=-1):
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`str` (:class:`bytes`
        in python 3). If `size` is negative or omitted all data is read.

        :Parameters:
          - `size` (optional): the number of bytes to read
        """
        self._ensure_file()

        if size == 0:
            return EMPTY

        remainder = int(self.length) - self.__position
        if size < 0 or size > remainder:
            size = remainder

        received = 0
        data = StringIO()
        while received < size:
            chunk_data = self.readchunk()
            received += len(chunk_data)
            data.write(chunk_data)

        # Detect extra chunks.
        max_chunk_n = math.ceil(self.length / float(self.chunk_size))
        chunk = self.__chunks.find_one({"files_id": self._id,
                                        "n": {"$gte": max_chunk_n}})
        # According to spec, ignore extra chunks if they are empty.
        if chunk is not None and len(chunk['data']):
            raise CorruptGridFile(
                "Extra chunk found: expected %i chunks but found "
                "chunk with n=%i" % (max_chunk_n, chunk['n']))

        self.__position -= received - size

        # Return 'size' bytes and store the rest.
        data.seek(size)
        self.__buffer = data.read()
        data.seek(0)
        return data.read(size)

    def readline(self, size=-1):
        """Read one line or up to `size` bytes from the file.

        :Parameters:
         - `size` (optional): the maximum number of bytes to read
        """
        if size == 0:
            return b''

        remainder = int(self.length) - self.__position
        if size < 0 or size > remainder:
            size = remainder

        received = 0
        data = StringIO()
        while received < size:
            chunk_data = self.readchunk()
            pos = chunk_data.find(NEWLN, 0, size)
            if pos != -1:
                size = received + pos + 1

            received += len(chunk_data)
            data.write(chunk_data)
            if pos != -1:
                break

        self.__position -= received - size

        # Return 'size' bytes and store the rest.
        data.seek(size)
        self.__buffer = data.read()
        data.seek(0)
        return data.read(size)

    def tell(self):
        """Return the current position of this file.
        """
        return self.__position

    def seek(self, pos, whence=_SEEK_SET):
        """Set the current position of this file.

        :Parameters:
         - `pos`: the position (or offset if using relative
           positioning) to seek to
         - `whence` (optional): where to seek
           from. :attr:`os.SEEK_SET` (``0``) for absolute file
           positioning, :attr:`os.SEEK_CUR` (``1``) to seek relative
           to the current position, :attr:`os.SEEK_END` (``2``) to
           seek relative to the file's end.
        """
        if whence == _SEEK_SET:
            new_pos = pos
        elif whence == _SEEK_CUR:
            new_pos = self.__position + pos
        elif whence == _SEEK_END:
            new_pos = int(self.length) + pos
        else:
            raise IOError(22, "Invalid value for `whence`")

        if new_pos < 0:
            raise IOError(22, "Invalid value for `pos` - must be positive")

        self.__position = new_pos
        self.__buffer = EMPTY

    def __iter__(self):
        """Return an iterator over all of this file's data.

        The iterator will return chunk-sized instances of
        :class:`str` (:class:`bytes` in python 3). This can be
        useful when serving files using a webserver that handles
        such an iterator efficiently.
        """
        return GridOutIterator(self, self.__chunks)

    def close(self):
        """Make GridOut more generically file-like."""
        pass

    def __enter__(self):
        """Makes it possible to use :class:`GridOut` files
        with the context manager protocol.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Makes it possible to use :class:`GridOut` files
        with the context manager protocol.
        """
        return False


class GridOutIterator(object):
    def __init__(self, grid_out, chunks):
        self.__id = grid_out._id
        self.__chunks = chunks
        self.__current_chunk = 0
        self.__max_chunk = math.ceil(float(grid_out.length) /
                                     grid_out.chunk_size)

    def __iter__(self):
        return self

    def next(self):
        if self.__current_chunk >= self.__max_chunk:
            raise StopIteration
        chunk = self.__chunks.find_one({"files_id": self.__id,
                                        "n": self.__current_chunk})
        if not chunk:
            raise CorruptGridFile("no chunk #%d" % self.__current_chunk)
        self.__current_chunk += 1
        return bytes(chunk["data"])

    __next__ = next


class GridOutCursor(Cursor):
    """A cursor / iterator for returning GridOut objects as the result
    of an arbitrary query against the GridFS files collection.
    """
    def __init__(self, collection, filter=None, skip=0, limit=0,
                 no_cursor_timeout=False, sort=None, batch_size=0):
        """Create a new cursor, similar to the normal
        :class:`~pymongo.cursor.Cursor`.

        Should not be called directly by application developers - see
        the :class:`~gridfs.GridFS` method :meth:`~gridfs.GridFS.find` instead.

        .. versionadded 2.7

        .. mongodoc:: cursors
        """
        # Hold on to the base "fs" collection to create GridOut objects later.
        self.__root_collection = collection

        super(GridOutCursor, self).__init__(
            collection.files, filter, skip=skip, limit=limit,
            no_cursor_timeout=no_cursor_timeout, sort=sort,
            batch_size=batch_size)

    def next(self):
        """Get next GridOut object from cursor.
        """
        # Work around "super is not iterable" issue in Python 3.x
        next_file = super(GridOutCursor, self).next()
        return GridOut(self.__root_collection, file_document=next_file)

    __next__ = next

    def add_option(self, *args, **kwargs):
        raise NotImplementedError("Method does not exist for GridOutCursor")

    def remove_option(self, *args, **kwargs):
        raise NotImplementedError("Method does not exist for GridOutCursor")

    def _clone_base(self):
        """Creates an empty GridOutCursor for information to be copied into.
        """
        return GridOutCursor(self.__root_collection)
