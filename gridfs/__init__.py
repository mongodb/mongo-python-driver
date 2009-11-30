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

"""GridFS is a specification for storing large objects in Mongo.

The `gridfs` package is an implementation of GridFS on top of `pymongo`,
exposing a file-like interface.
"""

import types

from grid_file import GridFile
from pymongo.database import Database

class GridFS(object):
    """An instance of GridFS on top of a single `pymongo.database.Database`.
    """
    def __init__(self, database):
        """Create a new instance of GridFS.

        Raises TypeError if database is not an instance of
        `pymongo.database.Database`.

        :Parameters:
          - `database`: database to use
        """
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        self.__database = database

    def open(self, filename, mode="r", collection="fs"):
        """Open a GridFile for reading or writing.

        Shorthand method for creating / opening a GridFile from a filename. mode
        must be a mode supported by `gridfs.grid_file.GridFile`.

        Only a single opened GridFile instance may exist for a file in gridfs
        at any time. Care must be taken to close GridFile instances when done
        using them. GridFiles support the context manager protocol (the "with"
        statement).

        :Parameters:
          - `filename`: name of the GridFile to open
          - `mode` (optional): mode to open the file in
          - `collection` (optional): root collection to use for this file
        """
        return GridFile({"filename": filename}, self.__database, mode, collection)

    def remove(self, filename_or_spec, collection="fs"):
        """Remove one or more GridFile(s).

        Can remove by filename, or by an entire file spec (see
        `gridfs.grid_file.GridFile` for documentation on valid fields. Delete
        all GridFiles that match filename_or_spec. Raises TypeError if
        filename_or_spec is not an instance of (str, unicode, dict, SON) or
        collection is not an instance of (str, unicode).

        :Parameters:
          - `filename_or_spec`: identifier of file(s) to remove
          - `collection` (optional): root collection where this file is located
        """
        spec = filename_or_spec
        if isinstance(filename_or_spec, types.StringTypes):
            spec = {"filename": filename_or_spec}
        if not isinstance(spec, dict):
            raise TypeError("filename_or_spec must be an "
                            "instance of (str, dict, SON)")
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")

        # convert to _id's so we can uniquely create GridFile instances
        ids = []
        for grid_file in self.__database[collection].files.find(spec):
            ids.append(grid_file["_id"])

        # open for writing to remove the chunks for these files
        for file_id in ids:
            f = GridFile({"_id": file_id}, self.__database, "w", collection)
            f.close()

        self.__database[collection].files.remove(spec)

    def list(self, collection="fs"):
        """List the names of all GridFiles stored in this instance of GridFS.

        Raises TypeError if collection is not an instance of (str, unicode).

        :Parameters:
          - `collection` (optional): root collection to list files from
        """
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")
        names = []
        for grid_file in self.__database[collection].files.find():
            names.append(grid_file["filename"])
        return names
