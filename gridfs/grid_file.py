# Copyright 2009-2010 10gen, Inc.
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
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO
from threading import Condition

from gridfs.errors import CorruptGridFile
from pymongo import ASCENDING
from pymongo.binary import Binary
from pymongo.database import Database
from pymongo.dbref import DBRef
from pymongo.objectid import ObjectId
from pymongo.son import SON

try:
    _SEEK_SET = os.SEEK_SET
    _SEEK_CUR = os.SEEK_CUR
    _SEEK_END = os.SEEK_END
except AttributeError: # before 2.5
    _SEEK_SET = 0
    _SEEK_CUR = 1
    _SEEK_END = 2

# TODO we should use per-file reader-writer locks here instead,
# for performance. Unfortunately they aren't in the Python standard library.
_files_lock = Condition()
_open_files = {}


class GridFile(object):
    """A "file" stored in GridFS.
    """
    # TODO should be able to create a GridFile given a Collection object instead
    # of a database and collection name?
    # TODO this whole file_spec thing is over-engineered. ought to be just
    # filename.
    def __init__(self, file_spec, database, mode="r", collection="fs"):
        """Open a "file" in GridFS.

        Application developers should generally not need to
        instantiate this class directly - instead see the
        :meth:`~gridfs.GridFS.open` method.

        Only a single opened :class:`GridFile` instance may exist for
        a file in gridfs at any time. Care must be taken to close
        :class:`GridFile` instances when done using
        them. :class:`GridFile` instances support the context manager
        protocol (the "with" statement).

        Raises :class:`TypeError` if `file_spec` is not an instance of
        :class:`dict`, `database` is not an instance of
        :class:`~pymongo.database.Database`, or `collection` is not an
        instance of :class:`basestring`.

        The `file_spec` argument must be a query specifying the file
        to open. The *first* file matching the query will be
        opened. If no such files exist, a new file is created using
        the metadata in `file_spec`.  The valid fields in a
        `file_spec` are as follows:

          - ``"_id"``: unique ID for this file (default:
            :class:`~pymongo.objectid.ObjectId`)

          - ``"filename"``: human name for the file

          - ``"contentType"``: valid mime-type for the file

          - ``"length"``: size of the file, in bytes

            .. note:: only used for querying, automatically set for inserts

          - ``"chunkSize"``: size of each of the chunks, in bytes
            (default: 256 kb)

          - ``"uploadDate"``: date when the object was first stored

            .. note:: only used for querying, automatically set for inserts

          - ``"aliases"``: array of alias strings

          - ``"metadata"``: document containing arbitrary metadata

        :Parameters:
          - `file_spec`: query specifier as described above
          - `database`: the database to store/retrieve this file in
          - `mode` (optional): the mode to open this file with, one of
            (``"r"``, ``"w"``)
          - `collection` (optional): the collection in which to
            store/retrieve this file
        """
        if not isinstance(file_spec, dict):
            raise TypeError("file_spec must be an instance of (dict, SON)")
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of database")
        if not isinstance(collection, basestring):
            raise TypeError("collection must be an instance of basestring")
        if not isinstance(mode, basestring):
            raise TypeError("mode must be an instance of basestring")
        if mode not in ("r", "w"):
            raise ValueError("mode must be one of ('r', 'w')")

        self.__collection = database[collection]
        self.__collection.chunks.ensure_index([("files_id", ASCENDING),
                                               ("n", ASCENDING)], unique=True)

        _files_lock.acquire()

        grid_file = self.__collection.files.find_one(file_spec)
        if grid_file:
            self.__id = grid_file["_id"]
        else:
            if mode == "r":
                _files_lock.release()
                raise IOError("No such file: %r" % file_spec)
            grid_file = file_spec.copy()
            grid_file["length"] = 0
            grid_file["uploadDate"] = datetime.datetime.utcnow()
            grid_file.setdefault("chunkSize", 256000)
            self.__id = self.__collection.files.insert(grid_file)

        # we use repr(self.__id) here because we need it to be string and
        # filename gets tricky with renaming. this is a hack.
        while repr(self.__id) in _open_files:
            _files_lock.wait()
        _open_files[repr(self.__id)] = True
        _files_lock.release()

        self.__mode = mode
        if mode == "w":
            self.__erase()
        self.__buffer = ""
        self.__write_buffer = StringIO()
        self.__position = 0
        self.__chunk_number = 0
        self.__chunk_size = grid_file["chunkSize"]
        self.__closed = False

    def __erase(self):
        """Erase all of the data stored in this GridFile.
        """
        grid_file = self.__collection.files.find_one({"_id": self.__id})
        grid_file["next"] = None
        grid_file["length"] = 0
        self.__collection.files.save(grid_file)

        self.__collection.chunks.remove({"files_id": self.__id})

    @property
    def _id(self):
        """The ``"_id"`` value for this :class:`GridFile`.

        .. versionadded:: 1.5.1
        """
        return self.__id

    @property
    def closed(self):
        """Is this :class:`GridFile` closed?
        """
        return self.__closed

    @property
    def mode(self):
        """Mode this :class:`GridFile` was opened with.
        """
        return self.__mode

    def __create_property(field_name, docstring, read_only=False):
        def getter(self):
            return self.__collection.files.find_one({"_id": self.__id}).get(field_name, None)
        def setter(self, value):
            grid_file = self.__collection.files.find_one({"_id": self.__id})
            grid_file[field_name] = value
            self.__collection.files.save(grid_file)

        if not read_only:
            return property(getter, setter, doc=docstring)
        return property(getter, doc=docstring)

    name = __create_property("filename", "Name of this :class:`GridFile`.",
                             True)
    content_type = __create_property("contentType", "Mime-type for "
                                     "this :class:`GridFile`.")
    length = __create_property("length", "Length (in bytes) of this "
                               ":class:`GridFile`.", True)
    chunk_size = __create_property("chunkSize", "Chunk size for this "
                                   ":class:`GridFile`.", True)
    upload_date = __create_property("uploadDate", "Date that this "
                                    ":class:`GridFile` was first uploaded.",
                                    True)
    aliases = __create_property("aliases", "List of aliases for this "
                                ":class:`GridFile`.")
    metadata = __create_property("metadata", "Metadata attached to this "
                                 ":class:`GridFile`.")
    md5 = __create_property("md5", "MD5 of the contents of this "
                            ":class:`GridFile` (generated on the server).",
                            True)

    def rename(self, filename):
        """Rename this :class:`GridFile`.

        Due to buffering, the rename might not actually occur until
        :meth:`flush` or :meth:`close` is called.

        :Parameters:
          - `filename`: the new name for this :class:`GridFile`
        """
        grid_file = self.__collection.files.find_one({"_id": self.__id})
        grid_file["filename"] = filename
        self.__collection.files.save(grid_file)

    def __flush_write_buffer(self):
        """Flush the write buffer contents out to a chunk.
        """
        data = self.__write_buffer.getvalue()

        if not data:
            return

        assert(len(data) <= self.__chunk_size)

        chunk = {"files_id": self.__id,
                 "n": self.__chunk_number,
                 "data": Binary(data) }

        self.__collection.chunks.update({"files_id": self.__id,
                                         "n": self.__chunk_number},
                                        chunk,
                                        upsert=True)

        if len(data) == self.__chunk_size:
            self.__chunk_number += 1
            self.__position += len(data)
            self.__write_buffer.close()
            self.__write_buffer = StringIO()

    def flush(self):
        """Flush the :class:`GridFile` to the database.
        """
        self.__assert_open()
        if self.mode != "w":
            return

        self.__flush_write_buffer()

        md5 = self.__collection.database.command(SON([("filemd5", self.__id),
                                                      ("root", self.__collection.name)]))["md5"]

        grid_file = self.__collection.files.find_one({"_id": self.__id})
        grid_file["md5"] = md5
        grid_file["length"] = self.__position + self.__write_buffer.tell()
        self.__collection.files.save(grid_file)

    def close(self):
        """Close the :class:`GridFile`.

        A closed :class:`GridFile` cannot be read or written any
        more. Calling :meth:`close` more than once is allowed.
        """
        if not self.__closed:
            self.flush()
        self.__closed = True

        _files_lock.acquire()
        if repr(self.__id) in _open_files:
            del _open_files[repr(self.__id)]
            _files_lock.notifyAll()
        _files_lock.release()

    def __assert_open(self, mode=None):
        if mode and self.mode != mode:
            raise ValueError("file must be open in mode %r" % mode)
        if self.closed:
            raise ValueError("operation cannot be performed on a closed GridFile")

    def read(self, size=-1):
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`str`. If
        `size` is negative or omitted all data is read. Raises
        :class:`ValueError` if this :class:`GridFile` is already
        closed.

        :Parameters:
          - `size` (optional): the number of bytes to read
        """
        self.__assert_open("r")

        if size == 0:
            return ""

        remainder = int(self.length) - self.__position
        if size < 0 or size > remainder:
            size = remainder

        bytes = self.__buffer
        chunk_number = (len(bytes) + self.__position) / self.__chunk_size

        while len(bytes) < size:
            chunk = self.__collection.chunks.find_one({"files_id": self.__id, "n": chunk_number})
            if not chunk:
                raise CorruptGridFile("no chunk for n = " + chunk_number)

            if not bytes:
                bytes += chunk["data"][self.__position % self.__chunk_size:]
            else:
                bytes += chunk["data"]

            chunk_number += 1

        self.__position += size
        to_return = bytes[:size]
        self.__buffer = bytes[size:]
        return to_return

    # TODO should support writing unicode to a file. this means that files will
    # need to have an encoding attribute.
    def write(self, data):
        """Write a string to the :class:`GridFile`. There is no return
        value.

        Due to buffering, the string may not actually show up in the
        database until the :meth:`flush` or :meth:`close` method is
        called. Raises :class:`ValueError` if this :class:`GridFile`
        is already closed. Raises :class:`TypeErrer` if `data` is not
        an instance of :class:`str`.

        :Parameters:
          - `data`: string of bytes to be written to the file
        """
        self.__assert_open("w")

        if not isinstance(data, str):
            raise TypeError("can only write strings")

        while data:
            space = self.__chunk_size - self.__write_buffer.tell()

            if len(data) <= space:
                self.__write_buffer.write(data)
                break
            else:
                self.__write_buffer.write(data[:space])
                self.__flush_write_buffer()
                data = data[space:]

    def tell(self):
        """Return the current position of this :class:`GridFile`
        (read-mode files only).
        """
        self.__assert_open("r")
        return self.__position

    def seek(self, pos, whence=_SEEK_SET):
        """Set the current position of this :class:`GridFile`
        (read-mode files only).

        :Parameters:
         - `pos`: the position (or offset if using relative
           positioning) to seek to
         - `whence` (optional): where to seek
           from. :attr:`os.SEEK_SET` (``0``) for absolute file
           positioning, :attr:`os.SEEK_CUR` (``1``) to seek relative
           to the current position, :attr:`os.SEEK_END` (``2``) to
           seek relative to the file's end.
        """
        self.__assert_open("r")
        if whence == _SEEK_SET:
            new_pos = pos
        elif whence == _SEEK_CUR:
            new_pos = self.__position + pos
        elif whence == _SEEK_END:
            new_pos = int(self.length) + pos
        else:
            raise IOError(22, "Invalid argument")

        if new_pos < 0:
            raise IOError(22, "Invalid argument")

        self.__position = new_pos
        self.__buffer = ""

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

        Close the file and allow exceptions to propogate.
        """
        self.close()
        return False # propogate exceptions
