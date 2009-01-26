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

from pymongo.son import SON
from pymongo.database import Database
from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef
from errors import CorruptGridFile

class GridFile(object):
    """A "file" stored in GridFS.
    """
    # TODO should be able to create a GridFile given a Collection object instead
    # of a database and collection name?
    def __init__(self, file_spec, database, mode="r", collection="_files"):
        """Open a "file" in GridFS.

        Application developers should not need to instantiate this class
        directly.

        Raises TypeError if file_spec is not an instance of (dict,
        `pymongo.son.SON`), database is not an instance of
        `pymongo.database.Database`, or collection is not an instance of (str,
        unicode).

        The file_spec argument must be a SON query specifier for the file to
        open. The first file matching the specifier will be opened. If no such
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

        Arguments:
        - `file_spec`: query specifier as described above
        - `database`: the database to store/retrieve this file in
        - `mode` (optional): the mode to open this file with, one of ("r", "w")
        - `collection` (optional): the collection to store/retrieve this file in
        """
        if not isinstance(file_spec, (types.DictType, SON)):
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

        file = self.__collection.find_one(file_spec)
        if file:
            self.__id = file["_id"]
        else:
            file_spec["length"] = 0
            file_spec["uploadDate"] = datetime.datetime.now()
            file_spec.setdefault("chunkSize", 256000)
            self.__id = self.__collection.insert(file_spec)["_id"]

        self.__mode = mode
        if mode == "w":
            self.__erase()
            self.__current_chunk = None
        self.__closed = False

    def __erase(self):
        """Erase all of the data stored in this GridFile.
        """
        file = self.__collection.find_one(self.__id)

        next = file.get("next", None)
        chunk_number = 0

        while next:
            chunk = self.__collection.database().dereference(next)
            if not chunk:
                raise CorruptGridFile("could not dereference: %r" % next)
            if chunk["cn"] != chunk_number:
                raise CorruptGridFile("incorrect chunk number: %r, should be: %r" %
                                      (chunk["cn"], chunk_number))

            self.__collection.database()[next.collection()].remove({"_id": next.id()})

            chunk_number += 1
            next = chunk["next"]

        file["next"] = None
        file["length"] = 0
        self.__collection.save(file)

    @property
    def closed(self):
        return self.__closed

    @property
    def mode(self):
        return self.__mode

    def __create_property(field_name, read_only=False):
        def get(self):
            return self.__collection.find_one(self.__id).get(field_name, None)
        def set(self, value):
            file = self.__collection.find_one(self.__id)
            file[field_name] = value
            self.__collection.save(file)
        if not read_only:
            return property(get, set)
        return property(get)

    name = __create_property("filename")
    content_type = __create_property("contentType")
    length = __create_property("length", True)
    chunk_size = __create_property("filename", True)
    upload_date = __create_property("uploadDate", True)
    aliases = __create_property("aliases")
    next = __create_property("next", True)

    __chunks_collection = "_chunks"
    def __write_current_chunk(self):
        # TODO _chunks collection should be configurable?
        self.__collection.database()[self.__chunks_collection].save(self.__current_chunk)

    def flush(self):
        """Flush the GridFile to the database.
        """
        self.__assert_open()
        if self.mode != "w" or not self.__current_chunk:
            return

        file = self.__collection.find_one(self.__id)

        length = file["chunkSize"] * self.__current_chunk["cn"] + len(self.__current_chunk["data"])
        file["length"] = length

        self.__write_current_chunk()
        self.__collection.save(file)

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

        Arguments:
        - `size` (optional): the number of bytes to read
        """
        self.__assert_open("r")

        if size < 0 or size > self.length:
            size = self.length

        bytes = ""
        next = self.next
        chunk_number = 0
        while len(bytes) < size:
            if not next:
                raise CorruptGridFile("incorrect length for file: %r" % self)
            chunk = self.__collection.database().dereference(next)
            if not chunk:
                raise CorruptGridFile("could not dereference: %r" % next)
            if chunk["cn"] != chunk_number:
                raise CorruptGridFile("incorrect chunk number: %r, should be: %r" %
                                      (chunk["cn"], chunk_number))

            bytes += chunk["data"]

            chunk_number += 1
            next = chunk["next"]

        bytes = bytes[:size]
        return bytes

    # TODO should support writing unicode to a file. this means that files will
    # need to have an encoding attribute.
    def write(self, str):
        """Write a string to the GridFile. There is no return value.

        Due to buffering, the string may not actually show up in the database
        until the `flush()` or `close()` method is called. Raises ValueError if
        this GridFile is already closed. Raises TypeErrer if str is not an
        instance of str.

        Arguments:
        - `str`: string of bytes to be written to the file
        """
        self.__assert_open("w")

        if not isinstance(str, types.StringType):
            raise TypeError("can only write strings")

        if not len(str):
            return

        def initialize_chunk(number):
            id = ObjectId()
            new_chunk = SON([("_id", id),
                             ("cn", number),
                             ("data", ""),
                             ("next", None)])
            return new_chunk

        if not self.__current_chunk:
            self.__current_chunk = initialize_chunk(0)
            ref = DBRef(self.__chunks_collection, self.__current_chunk["_id"])
            file = self.__collection.find_one(self.__id)
            file["next"] = ref
            self.__collection.save(file)

        data = self.__current_chunk["data"]
        data += str

        while len(data) > self.chunk_size:
            self.__current_chunk["data"] = data[:self.chunk_size]
            data = data[self.chunk_size:]

            new_chunk = initialize_chunk(self.__current_chunk["cn"] + 1)

            self.__current_chunk["next"] = DBRef(self.__chunks_collection, new_chunk["_id"])
            self.__write_current_chunk()
            self.__current_chunk = new_chunk

        self.__current_chunk["data"] = data

    def writelines(self, sequence):
        """Write a sequence of strings to the file.

        Does not add seperators.
        """
        for line in sequence:
            self.write(line)
