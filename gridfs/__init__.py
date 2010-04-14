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

from gridfs.errors import (NoFile,
                           UnsupportedAPI)
from gridfs.grid_file import (GridIn,
                              GridOut)
from pymongo import (ASCENDING,
                     DESCENDING)
from pymongo.database import Database

class GridFS(object):
    """An instance of GridFS on top of a single Database.
    """
    def __init__(self, database, collection="fs"):
        """Create a new instance of :class:`GridFS`.

        Raises :class:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.

        :Parameters:
          - `database`: database to use
          - `collection` (optional): root collection to use

        .. versionadded:: 1.6
           The `collection` parameter.

        .. mongodoc:: gridfs
        """
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        self.__database = database
        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks
        self.__chunks.create_index([("files_id", ASCENDING), ("n", ASCENDING)],
                                   unique=True)

    def new_file(self, **kwargs):
        """Create a new file in GridFS.

        Returns a new :class:`~gridfs.grid_file.GridIn` instance to
        which data can be written. Any keyword arguments will be
        passed through to :meth:`~gridfs.grid_file.GridIn`.

        :Parameters:
          - `**kwargs` (optional): keyword arguments for file creation

        .. versionadded:: 1.6
        """
        return GridIn(self.__collection, **kwargs)

    def put(self, data, **kwargs):
        """Put data in GridFS as a new file.

        Equivalent to doing:

        >>> f = new_file(**kwargs)
        >>> try:
        >>>     f.write(data)
        >>> finally:
        >>>     f.close()

        `data` can be either an instance of :class:`str` or a
        file-like object providing a :meth:`read` method. Any keyword
        arguments will be passed through to the created file - see
        :meth:`~gridfs.grid_file.GridIn` for possible
        arguments. Returns the ``"_id"`` of the created file.

        :Parameters:
          - `data`: data to be written as a file.
          - `**kwargs` (optional): keyword arguments for file creation

        .. versionadded:: 1.6
        """
        grid_file = GridIn(self.__collection, **kwargs)
        try:
            grid_file.write(data)
        finally:
            grid_file.close()
        return grid_file._id

    def get(self, file_id):
        """Get a file from GridFS by ``"_id"``.

        Returns an instance of :class:`~gridfs.grid_file.GridOut`,
        which provides a file-like interface for reading.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to get

        .. versionadded:: 1.6
        """
        return GridOut(self.__collection, file_id)

    def get_last_version(self, filename):
        """Get a file from GridFS by ``"filename"``.

        Returns the most recently uploaded file in GridFS with the
        name `filename` as an instance of
        :class:`~gridfs.grid_file.GridOut`. Raises
        :class:`~gridfs.errors.NoFile` if no such file exists.

        An index on ``{filename: 1, uploadDate: -1}`` will
        automatically be created when this method is called the first
        time.

        :Parameters:
          - `filename`: ``"filename"`` of the file to get

        .. versionadded:: 1.6
        """
        self.__files.ensure_index([("filename", ASCENDING),
                                   ("uploadDate", DESCENDING)])

        cursor = self.__files.find({"filename": filename})
        cursor.limit(-1).sort("uploadDate", DESCENDING)
        try:
            grid_file = cursor.next()
            return GridOut(self.__collection, grid_file["_id"])
        except StopIteration:
            raise NoFile("no file in gridfs with filename %r" % filename)

    # TODO add optional safe mode for chunk removal?
    def delete(self, file_id):
        """Delete a file from GridFS by ``"_id"``.

        Removes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to delete

        .. versionadded:: 1.6
        """
        self.__files.remove({"_id": file_id}, safe=True)
        self.__chunks.remove({"files_id": file_id})

    def list(self):
        """List the names of all files stored in this instance of
        :class:`GridFS`.

        .. versionchanged:: 1.6
           Removed the `collection` argument.
        """
        return self.__files.distinct("filename")

    def open(self, *args, **kwargs):
        """No longer supported.

        .. versionchanged:: 1.6
           The open method is no longer supported.
        """
        raise UnsupportedAPI("The open method is no longer supported.")

    def remove(self, *args, **kwargs):
        """No longer supported.

        .. versionchanged:: 1.6
           The remove method is no longer supported.
        """
        raise UnsupportedAPI("The remove method is no longer supported. "
                             "Please use the delete method instead.")
