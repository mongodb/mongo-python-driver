# Copyright 2009 10gen, Inc.
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

"""File-like object used for reading from and writing to GridFS"""

import types
import datetime
import math

from pymongo.son import SON
from pymongo.database import Database
from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef
from pymongo.binary import Binary
from errors import CorruptGridFile
from pymongo import ASCENDING

class GridFile(object):
    """A "file" stored in GridFS.
    """
    # TODO should be able to create a GridFile given a Collection object instead
    # of a database and collection name?
    def __init__(self, file_spec, database, mode="r", collection="fs"):
        """Open a "file" in GridFS.

        Application developers should generally not need to instantiate this
        class directly.

        Raises TypeError if file_spec is not an instance of dict, database is
        not an instance of `pymongo.database.Database`, or collection is not an
        instance of (str, unicode).

        The file_spec argument must be a SON query specifier for the file to
        open. The *first* file matching the specifier will be opened. If no such
        files exist, a new file is created using the metadata in file_spec.
        The valid fields in a file_spec are as follows:

          - "_id": unique ID for this file
            * default: `pymongo.objectid.ObjectId()`
          - "filename": human name for the file
          - "contentType": valid mime-type for the file
          - "length": size of the file, in bytes
            * only used for querying, automatically set for inserts
          - "chunkSize": size of each of the chunks, in bytes
            * default: 256 kb
          - "uploadDate": date when the object was first stored
            * only used for querying, automatically set for inserts
          - "aliases": array of alias strings
          - "metadata": a SON document containing arbitrary data

        :Parameters:
          - `file_spec`: query specifier as described above
          - `database`: the database to store/retrieve this file in
          - `mode` (optional): the mode to open this file with, one of
            ("r", "w")
          - `collection` (optional): the collection in which to store/retrieve
            this file
        """
        if not isinstance(file_spec, types.DictType):
            raise TypeError("file_spec must be an instance of (dict, SON)")
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of database")
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")
        if not isinstance(mode, types.StringTypes):
            raise TypeError("mode must be an instance of (str, unicode)")
        if mode not in ("r", "w"):
            raise ValueError("mode must be one of ('r', 'w')")

        self.__collection = database[collection]
        self.__collection.chunks.create_index([("files_id", ASCENDING), ("n", ASCENDING)])

        grid_file = self.__collection.files.find_one(file_spec)
        if grid_file:
            self.__id = grid_file["_id"]
        else:
            if mode == "r":
                raise IOError("No such file: %r" % file_spec)
            file_spec["length"] = 0
            file_spec["uploadDate"] = datetime.datetime.now()
            file_spec.setdefault("chunkSize", 256000)
            self.__id = self.__collection.files.insert(file_spec)["_id"]

        self.__mode = mode
        if mode == "w":
            self.__erase()
        self.__buffer = ""
        self.__position = 0
        self.__chunk_number = 0
        self.__closed = False

    def __erase(self):
        """Erase all of the data stored in this GridFile.
        """
        grid_file = self.__collection.files.find_one(self.__id)
        grid_file["next"] = None
        grid_file["length"] = 0
        self.__collection.files.save(grid_file)

        self.__collection.chunks.remove({"files_id": self.__id})

    @property
    def closed(self):
        return self.__closed

    @property
    def mode(self):
        return self.__mode

    def __create_property(field_name, read_only=False):
        def getter(self):
            return self.__collection.files.find_one(self.__id).get(field_name, None)
        def setter(self, value):
            grid_file = self.__collection.files.find_one(self.__id)
            grid_file[field_name] = value
            self.__collection.files.save(grid_file)
        if not read_only:
            return property(getter, setter)
        return property(getter)

    name = __create_property("filename", True)
    content_type = __create_property("contentType")
    length = __create_property("length", True)
    chunk_size = __create_property("chunkSize", True)
    upload_date = __create_property("uploadDate", True)
    aliases = __create_property("aliases")
    metadata = __create_property("metadata")

    def rename(self, filename):
        """Rename this GridFile.

        Due to buffering, the rename might not actually occur until `flush()` or
        `close()` is called.

        :Parameters:
          - `filename`: the new name for this GridFile
        """
        grid_file = self.__collection.files.find_one(self.__id)
        grid_file["filename"] = filename
        self.__collection.files.save(grid_file)

    def __max_chunk(self):
        return self.__collection.chunks.find_one({"files_id": self.__id, "n": self.__chunk_number})

    def __new_chunk(self, n):
        chunk = {"files_id": self.__id,
                 "n": n,
                 "data": ""}
        self.__collection.chunks.insert(chunk)
        return chunk

    def __write_buffer_to_chunks(self):
        """Write the buffer contents out to chunks.
        """
        while len(self.__buffer):
            max_chunk = self.__max_chunk()
            if not max_chunk:
                max_chunk = self.__new_chunk(self.__chunk_number)
            space = (self.__chunk_number + 1) * self.chunk_size - self.__position
            if not space:
                self.__chunk_number += 1
                max_chunk = self.__new_chunk(self.__chunk_number)
                space = self.chunk_size
            to_write = len(self.__buffer) > space and space or len(self.__buffer)

            max_chunk["data"] = Binary(max_chunk["data"] + self.__buffer[:to_write])
            self.__collection.chunks.save(max_chunk)
            self.__buffer = self.__buffer[to_write:]
            self.__position += to_write

    def flush(self):
        """Flush the GridFile to the database.
        """
        self.__assert_open()
        if self.mode != "w":
            return

        self.__write_buffer_to_chunks()

        grid_file = self.__collection.files.find_one(self.__id)
        grid_file["length"] = self.__position + len(self.__buffer)
        self.__collection.files.save(grid_file)

    def close(self):
        """Close the GridFile.

        A closed GridFile cannot be read or written any more. Calling `close()`
        more than once is allowed.
        """
        if not self.__closed:
            self.flush()
        self.__closed = True

    def __assert_open(self, mode=None):
        if mode and self.mode != mode:
            raise ValueError("file must be open in mode %r" % mode)
        if self.closed:
            raise ValueError("operation cannot be performed on a closed GridFile")

    def read(self, size=-1):
        """Read at most size bytes from the file (less if there isn't enough
        data).

        The bytes are returned as a string object. If size is negative or omitted
        all data is read. Raises ValueError if this GridFile is already closed.

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
        chunk_number = math.floor(self.__position / self.chunk_size)

        while len(bytes) < size:
            chunk = self.__collection.chunks.find_one({"files_id": self.__id, "n": chunk_number})
            if not chunk:
                raise CorruptGridFile("no chunk for n = " + chunk_number)
            bytes += chunk["data"]
            chunk_number += 1

        self.__position += size
        to_return = bytes[:size]
        self.__buffer = bytes[size:]
        return to_return

    # TODO should support writing unicode to a file. this means that files will
    # need to have an encoding attribute.
    def write(self, str):
        """Write a string to the GridFile. There is no return value.

        Due to buffering, the string may not actually show up in the database
        until the `flush()` or `close()` method is called. Raises ValueError if
        this GridFile is already closed. Raises TypeErrer if str is not an
        instance of str.

        :Parameters:
          - `str`: string of bytes to be written to the file
        """
        self.__assert_open("w")

        if not isinstance(str, types.StringType):
            raise TypeError("can only write strings")

        if not len(str):
            return

        self.__buffer += str

    def writelines(self, sequence):
        """Write a sequence of strings to the file.

        Does not add seperators.
        """
        for line in sequence:
            self.write(line)
