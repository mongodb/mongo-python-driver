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

"""GridFS is a specification for storing large objects in Mongo.

The :mod:`gridfs` package is an implementation of GridFS on top of
:mod:`pymongo`, exposing a file-like interface.

.. mongodoc:: gridfs
"""

from gridfs.grid_file import GridFile
from pymongo.database import Database

class GridFS(object):
    """An instance of GridFS on top of a single Database.
    """
    def __init__(self, database):
        """Create a new instance of :class:`GridFS`.

        Raises :class:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.

        :Parameters:
          - `database`: database to use

        .. mongodoc:: gridfs
        """
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        self.__database = database

    def open(self, filename, mode="r", collection="fs"):
        """Open a :class:`~gridfs.grid_file.GridFile` for reading or
        writing.

        Shorthand method for creating / opening a
        :class:`~gridfs.grid_file.GridFile` with name
        `filename`. `mode` must be a mode supported by
        :class:`~gridfs.grid_file.GridFile`.

        Only a single opened :class:`~gridfs.grid_file.GridFile`
        instance may exist for a file in gridfs at any time. Care must
        be taken to close :class:`~gridfs.grid_file.GridFile`
        instances when done using
        them. :class:`~gridfs.grid_file.GridFile` instances support
        the context manager protocol (the "with" statement).

        :Parameters:
          - `filename`: name of the :class:`~gridfs.grid_file.GridFile`
            to open
          - `mode` (optional): mode to open the file in
          - `collection` (optional): root collection to use for this file
        """
        return GridFile({"filename": filename}, self.__database, mode, collection)

    def remove(self, filename_or_spec, collection="fs"):
        """Remove one or more :class:`~gridfs.grid_file.GridFile`
        instances.

        Can remove by filename, or by an entire file spec (see
        :meth:`~gridfs.grid_file.GridFile` for documentation on valid
        fields. Delete all :class:`~gridfs.grid_file.GridFile`
        instances that match `filename_or_spec`. Raises
        :class:`TypeError` if `filename_or_spec` is not an instance of
        (:class:`basestring`, :class:`dict`,
        :class:`~pymongo.son.SON`) or collection is not an instance of
        :class:`basestring`.

        :Parameters:
          - `filename_or_spec`: identifier of file(s) to remove
          - `collection` (optional): root collection where this file is located
        """
        spec = filename_or_spec
        if isinstance(filename_or_spec, basestring):
            spec = {"filename": filename_or_spec}
        if not isinstance(spec, dict):
            raise TypeError("filename_or_spec must be an "
                            "instance of (basestring, dict, SON)")
        if not isinstance(collection, basestring):
            raise TypeError("collection must be an instance of basestring")

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
        """List the names of all :class:`~gridfs.grid_file.GridFile`
        instances stored in this instance of :class:`GridFS`.

        Raises :class:`TypeError` if collection is not an instance of
        :class:`basestring`.

        :Parameters:
          - `collection` (optional): root collection to list files from
        """
        if not isinstance(collection, basestring):
            raise TypeError("collection must be an instance of basestring")
        names = []
        for grid_file in self.__database[collection].files.find():
            names.append(grid_file["filename"])
        return names
