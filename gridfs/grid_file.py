# Copyright 2009-2012 10gen, Inc.
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

from bson.binary import Binary
from bson.objectid import ObjectId
from bson.py3compat import b, binary_type, string_types, text_type, StringIO
from gridfs.errors import (CorruptGridFile,
                           FileExists,
                           NoFile,
                           UnsupportedAPI)
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

try:
    _SEEK_SET = os.SEEK_SET
    _SEEK_CUR = os.SEEK_CUR
    _SEEK_END = os.SEEK_END
# before 2.5
except AttributeError:
    _SEEK_SET = 0
    _SEEK_CUR = 1
    _SEEK_END = 2

EMPTY = b("")
NEWLN = b("\n")

"""Default chunk size, in bytes."""
DEFAULT_CHUNK_SIZE = 256 * 1024


def _create_property(field_name, docstring,
                      read_only=False, closed_only=False):
    """Helper for creating properties to read/write to files.
    """
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
            self._coll.files.update({"_id": self._file["_id"]},
                                    {"$set": {field_name: value}},
                                    **self._coll._get_wc_override())
        self._file[field_name] = value

    if read_only:
        docstring = docstring + "\n\nThis attribute is read-only."
    elif closed_only:
        docstring = "%s\n\n%s" % (docstring, "This attribute is read-only and "
                                  "can only be read after :meth:`close` "
                                  "has been called.")

    if not read_only and not closed_only:
        return property(getter, setter, doc=docstring)
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
            chunks, in bytes (default: 256 kb)

          - ``"encoding"``: encoding used for this file. In Python 2,
            any :class:`unicode` that is written to the file will be
            converted to a :class:`str`. In Python 3, any :class:`str`
            that is written to the file will be converted to
            :class:`bytes`.

        If you turn off write-acknowledgment for performance reasons, it is
        critical to wrap calls to :meth:`write` and :meth:`close` within a
        single request:

           >>> from pymongo import MongoClient
           >>> from gridfs import GridFS
           >>> client = MongoClient(w=0) # turn off write acknowledgment
           >>> fs = GridFS(client)
           >>> gridin = fs.new_file()
           >>> request = client.start_request()
           >>> try:
           ...     for i in range(10):
           ...         gridin.write('foo')
           ...     gridin.close()
           ... finally:
           ...     request.end()

        In Python 2.5 and later this code can be simplified with a
        with-statement, see :doc:`/examples/requests` for more information.

        :Parameters:
          - `root_collection`: root collection to write to
          - `**kwargs` (optional): file level options (see above)
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an "
                            "instance of Collection")

        # Handle alternative naming
        if "content_type" in kwargs:
            kwargs["contentType"] = kwargs.pop("content_type")
        if "chunk_size" in kwargs:
            kwargs["chunkSize"] = kwargs.pop("chunk_size")

        # Defaults
        kwargs["_id"] = kwargs.get("_id", ObjectId())
        kwargs["chunkSize"] = kwargs.get("chunkSize", DEFAULT_CHUNK_SIZE)

        root_collection.chunks.ensure_index([("files_id", ASCENDING),
                                             ("n", ASCENDING)],
                                            unique=True)
        object.__setattr__(self, "_coll", root_collection)
        object.__setattr__(self, "_chunks", root_collection.chunks)
        object.__setattr__(self, "_file", kwargs)
        object.__setattr__(self, "_buffer", StringIO())
        object.__setattr__(self, "_position", 0)
        object.__setattr__(self, "_chunk_number", 0)
        object.__setattr__(self, "_closed", False)

    @property
    def closed(self):
        """Is this file closed?
        """
        return self._closed

    _id = _create_property("_id", "The ``'_id'`` value for this file.",
                            read_only=True)
    filename = _create_property("filename", "Name of this file.")
    name = _create_property("filename", "Alias for `filename`.")
    content_type = _create_property("contentType", "Mime-type for this file.")
    length = _create_property("length", "Length (in bytes) of this file.",
                               closed_only=True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                   read_only=True)
    upload_date = _create_property("uploadDate",
                                    "Date that this file was uploaded.",
                                    closed_only=True)
    md5 = _create_property("md5", "MD5 of the contents of this file "
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
                self._coll.files.update({"_id": self._file["_id"]},
                                        {"$set": {name: value}},
                                        **self._coll._get_wc_override())

    def __flush_data(self, data):
        """Flush `data` to a chunk.
        """
        if not data:
            return
        assert(len(data) <= self.chunk_size)

        chunk = {"files_id": self._file["_id"],
                 "n": self._chunk_number,
                 "data": Binary(data)}

        try:
            self._chunks.insert(chunk)
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

            db = self._coll.database

            # See PYTHON-417, "Sharded GridFS fails with exception: chunks out
            # of order." Inserts via mongos, even if they use a single
            # connection, can succeed out-of-order due to the writebackListener.
            # We mustn't call "filemd5" until all inserts are complete, which
            # we ensure by calling getLastError (and ignoring the result).
            db.error()

            md5 = db.command(
                "filemd5", self._id, root=self._coll.name)["md5"]

            self._file["md5"] = md5
            self._file["length"] = self._position
            self._file["uploadDate"] = datetime.datetime.utcnow()

            return self._coll.files.insert(self._file,
                                           **self._coll._get_wc_override())
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

        .. versionadded:: 1.9
           The ability to write :class:`unicode`, if the file has an
           :attr:`encoding` attribute.
        """
        if self._closed:
            raise ValueError("cannot write to a closed file")

        try:
            # file-like
            read = data.read
        except AttributeError:
            # string
            if not isinstance(data, string_types):
                raise TypeError("can only write strings or file-like objects")
            if isinstance(data, unicode):
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
                to_write = read(space)
                self._buffer.write(to_write)
                if len(to_write) < space:
                    return # EOF or incomplete
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
          - `file_id`: value of ``"_id"`` for the file to read
          - `file_document`: file document from `root_collection.files`

        .. versionadded:: 1.9
           The `file_document` parameter.
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an "
                            "instance of Collection")

        self.__chunks = root_collection.chunks

        files = root_collection.files
        self._file = file_document or files.find_one({"_id": file_id})

        if not self._file:
            raise NoFile("no file in gridfs collection %r with _id %r" %
                         (files, file_id))

        self.__buffer = EMPTY
        self.__position = 0

    _id = _create_property("_id", "The ``'_id'`` value for this file.", True)
    filename = _create_property("filename", "Name of this file.", True)
    name = _create_property("filename", "Alias for `filename`.", True)
    content_type = _create_property("contentType", "Mime-type for this file.",
                                     True)
    length = _create_property("length", "Length (in bytes) of this file.",
                               True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                   True)
    upload_date = _create_property("uploadDate",
                                    "Date that this file was first uploaded.",
                                    True)
    aliases = _create_property("aliases", "List of aliases for this file.",
                                True)
    metadata = _create_property("metadata", "Metadata attached to this file.",
                                 True)
    md5 = _create_property("md5", "MD5 of the contents of this file "
                            "(generated on the server).", True)

    def __getattr__(self, name):
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridOut object has no attribute '%s'" % name)

    def read(self, size=-1):
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`str` (:class:`bytes`
        in python 3). If `size` is negative or omitted all data is read.

        :Parameters:
          - `size` (optional): the number of bytes to read
        """
        if size == 0:
            return ""

        remainder = int(self.length) - self.__position
        if size < 0 or size > remainder:
            size = remainder

        received = len(self.__buffer)
        chunk_number = int((received + self.__position) / self.chunk_size)
        chunks = []

        while received < size:
            chunk = self.__chunks.find_one({"files_id": self._id,
                                            "n": chunk_number})
            if not chunk:
                raise CorruptGridFile("no chunk #%d" % chunk_number)

            if received:
                chunk_data = chunk["data"]
            else:
                chunk_data = chunk["data"][self.__position % self.chunk_size:]

            received += len(chunk_data)
            chunks.append(chunk_data)
            chunk_number += 1

        data = EMPTY.join([self.__buffer] + chunks)
        self.__position += size
        to_return = data[:size]
        self.__buffer = data[size:]
        return to_return

    def readline(self, size=-1):
        """Read one line or up to `size` bytes from the file.

        :Parameters:
         - `size` (optional): the maximum number of bytes to read

        .. versionadded:: 1.9
        """
        bytes = EMPTY
        while len(bytes) != size:
            byte = self.read(1)
            bytes += byte
            if byte == EMPTY or byte == NEWLN:
                break
        return bytes

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
        return binary_type(chunk["data"])


class GridFile(object):
    """No longer supported.

    .. versionchanged:: 1.6
       The GridFile class is no longer supported.
    """
    def __init__(self, *args, **kwargs):
        raise UnsupportedAPI("The GridFile class is no longer supported. "
                             "Please use GridIn or GridOut instead.")
