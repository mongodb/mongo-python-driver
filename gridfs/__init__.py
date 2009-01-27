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

    def open(self, filename, mode="r"):
        """Open a GridFile for reading or writing.

        Shorthand method for creating / opening a GridFile from a filename. mode
        must be a mode supported by `gridfs.grid_file.GridFile`

        :Parameters:
          - `filename`: the name of the GridFile to open
          - `mode` (optional): the mode to open the file in
        """
        return GridFile({"filename": filename}, self.__database, mode)

    def remove(self, filename_or_spec):
        """Remove one or more GridFile(s).

        Can remove by filename, or by an entire file spec (see
        `gridfs.grid_file.GridFile` for documentation on valid fields. Delete
        all GridFiles that match filename_or_spec. Raises TypeError if
        filename_or_spec is not an instance of (str, unicode, dict, SON).

        :Parameters:
          - `filename_or_spec`: identifier of file(s) to remove
        """
        spec = filename_or_spec
        if isinstance(filename_or_spec, types.StringTypes):
            spec = {"filename": filename_or_spec}

        # convert to _id's so we can uniquely create GridFile instances
        ids = []
        for file in self.__database._files.find(spec):
            ids.append(file["_id"])

        # open for writing to remove the chunks for these files
        for id in ids:
            f = GridFile({"_id": id}, self.__database, "w")
            f.close()

        self.__database._files.remove(spec)

    def list(self):
        """List the names of all GridFiles stored in this instance of GridFS.
        """
        names = []
        for file in self.__database._files.find():
            names.append(file["filename"])
        return names
