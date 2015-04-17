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

"""GridFS is a specification for storing large objects in Mongo.

The :mod:`gridfs` package is an implementation of GridFS on top of
:mod:`pymongo`, exposing a file-like interface.

.. mongodoc:: gridfs
"""

from collections import Mapping

from gridfs.errors import NoFile
from gridfs.grid_file import (GridIn,
                              GridOut,
                              GridOutCursor)
from pymongo import (ASCENDING,
                     DESCENDING)
from pymongo.database import Database
from pymongo.errors import ConfigurationError


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

        .. versionchanged:: 3.0
           `database` must use an acknowledged
           :attr:`~pymongo.database.Database.write_concern`

        .. mongodoc:: gridfs
        """
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        if not database.write_concern.acknowledged:
            raise ConfigurationError('database must use '
                                     'acknowledged write_concern')

        self.__database = database
        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks

    def __is_secondary(self):
        return not self.__database.client._is_writable()

    def __ensure_index_files_id(self):
        if not self.__is_secondary():
            self.__chunks.create_index([("files_id", ASCENDING),
                                        ("n", ASCENDING)],
                                       unique=True)

    def __ensure_index_filename(self):
        if not self.__is_secondary():
            self.__files.create_index([("filename", ASCENDING),
                                       ("uploadDate", DESCENDING)])

    def new_file(self, **kwargs):
        """Create a new file in GridFS.

        Returns a new :class:`~gridfs.grid_file.GridIn` instance to
        which data can be written. Any keyword arguments will be
        passed through to :meth:`~gridfs.grid_file.GridIn`.

        If the ``"_id"`` of the file is manually specified, it must
        not already exist in GridFS. Otherwise
        :class:`~gridfs.errors.FileExists` is raised.

        :Parameters:
          - `**kwargs` (optional): keyword arguments for file creation
        """
        # No need for __ensure_index_files_id() here; GridIn ensures
        # the (files_id, n) index when needed.
        return GridIn(self.__collection, **kwargs)

    def put(self, data, **kwargs):
        """Put data in GridFS as a new file.

        Equivalent to doing::

          try:
              f = new_file(**kwargs)
              f.write(data)
          finally:
              f.close()

        `data` can be either an instance of :class:`str` (:class:`bytes`
        in python 3) or a file-like object providing a :meth:`read` method.
        If an `encoding` keyword argument is passed, `data` can also be a
        :class:`unicode` (:class:`str` in python 3) instance, which will
        be encoded as `encoding` before being written. Any keyword arguments
        will be passed through to the created file - see
        :meth:`~gridfs.grid_file.GridIn` for possible arguments. Returns the
        ``"_id"`` of the created file.

        If the ``"_id"`` of the file is manually specified, it must
        not already exist in GridFS. Otherwise
        :class:`~gridfs.errors.FileExists` is raised.

        :Parameters:
          - `data`: data to be written as a file.
          - `**kwargs` (optional): keyword arguments for file creation

        .. versionchanged:: 3.0
           w=0 writes to GridFS are now prohibited.
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
        """
        gout = GridOut(self.__collection, file_id)

        # Raise NoFile now, instead of on first attribute access.
        gout._ensure_file()
        return gout

    def get_version(self, filename=None, version=-1, **kwargs):
        """Get a file from GridFS by ``"filename"`` or metadata fields.

        Returns a version of the file in GridFS whose filename matches
        `filename` and whose metadata fields match the supplied keyword
        arguments, as an instance of :class:`~gridfs.grid_file.GridOut`.

        Version numbering is a convenience atop the GridFS API provided
        by MongoDB. If more than one file matches the query (either by
        `filename` alone, by metadata fields, or by a combination of
        both), then version ``-1`` will be the most recently uploaded
        matching file, ``-2`` the second most recently
        uploaded, etc. Version ``0`` will be the first version
        uploaded, ``1`` the second version, etc. So if three versions
        have been uploaded, then version ``0`` is the same as version
        ``-3``, version ``1`` is the same as version ``-2``, and
        version ``2`` is the same as version ``-1``.

        Raises :class:`~gridfs.errors.NoFile` if no such version of
        that file exists.

        An index on ``{filename: 1, uploadDate: -1}`` will
        automatically be created when this method is called the first
        time.

        :Parameters:
          - `filename`: ``"filename"`` of the file to get, or `None`
          - `version` (optional): version of the file to get (defaults
            to -1, the most recent version uploaded)
          - `**kwargs` (optional): find files by custom metadata.
        """
        self.__ensure_index_filename()
        query = kwargs
        if filename is not None:
            query["filename"] = filename

        cursor = self.__files.find(query)
        if version < 0:
            skip = abs(version) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(version).sort("uploadDate", ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self.__collection, file_document=grid_file)
        except StopIteration:
            raise NoFile("no version %d for filename %r" % (version, filename))

    def get_last_version(self, filename=None, **kwargs):
        """Get the most recent version of a file in GridFS by ``"filename"``
        or metadata fields.

        Equivalent to calling :meth:`get_version` with the default
        `version` (``-1``).

        :Parameters:
          - `filename`: ``"filename"`` of the file to get, or `None`
          - `**kwargs` (optional): find files by custom metadata.
        """
        return self.get_version(filename=filename, **kwargs)

    # TODO add optional safe mode for chunk removal?
    def delete(self, file_id):
        """Delete a file from GridFS by ``"_id"``.

        Deletes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        .. note:: Deletes of non-existent files are considered successful
           since the end result is the same: no file with that _id remains.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to delete
        """
        self.__ensure_index_files_id()
        self.__files.delete_one({"_id": file_id})
        self.__chunks.delete_many({"files_id": file_id})

    def list(self):
        """List the names of all files stored in this instance of
        :class:`GridFS`.

        An index on ``{filename: 1, uploadDate: -1}`` will
        automatically be created when this method is called the first
        time.

        .. versionchanged:: 2.7
           ``list`` ensures an index, the same as ``get_version``.
        """
        self.__ensure_index_filename()

        # With an index, distinct includes documents with no filename
        # as None.
        return [
            name for name in self.__files.distinct("filename")
            if name is not None]

    def find_one(self, filter=None, *args, **kwargs):
        """Get a single file from gridfs.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single :class:`~gridfs.grid_file.GridOut`,
        or ``None`` if no matching file is found. For example::

            file = fs.find_one({"filename": "lisa.txt"})

        :Parameters:
          - `filter` (optional): a dictionary specifying
            the query to be performing OR any other type to be used as
            the value for a query for ``"_id"`` in the file collection.
          - `*args` (optional): any additional positional arguments are
            the same as the arguments to :meth:`find`.
          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to :meth:`find`.
        """
        if filter is not None and not isinstance(filter, Mapping):
            filter = {"_id": filter}

        for f in self.find(filter, *args, **kwargs):
            return f

        return None

    def find(self, *args, **kwargs):
        """Query GridFS for files.

        Returns a cursor that iterates across files matching
        arbitrary queries on the files collection. Can be combined
        with other modifiers for additional control. For example::

          for grid_out in fs.find({"filename": "lisa.txt"},
                                  no_cursor_timeout=True):
              data = grid_out.read()

        would iterate through all versions of "lisa.txt" stored in GridFS.
        Note that setting no_cursor_timeout to True may be important to
        prevent the cursor from timing out during long multi-file processing
        work.

        As another example, the call::

          most_recent_three = fs.find().sort("uploadDate", -1).limit(3)

        would return a cursor to the three most recently uploaded files
        in GridFS.

        Follows a similar interface to
        :meth:`~pymongo.collection.Collection.find`
        in :class:`~pymongo.collection.Collection`.

        :Parameters:
          - `filter` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `skip` (optional): the number of files to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `no_cursor_timeout` (optional): if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~gridfs.grid_file.GridOutCursor`
        corresponding to this query.

        .. versionchanged:: 3.0
           Removed the read_preference, tag_sets, and
           secondary_acceptable_latency_ms options.
        .. versionadded:: 2.7
        .. mongodoc:: find
        """
        return GridOutCursor(self.__collection, *args, **kwargs)

    def exists(self, document_or_id=None, **kwargs):
        """Check if a file exists in this instance of :class:`GridFS`.

        The file to check for can be specified by the value of its
        ``_id`` key, or by passing in a query document. A query
        document can be passed in as dictionary, or by using keyword
        arguments. Thus, the following three calls are equivalent:

        >>> fs.exists(file_id)
        >>> fs.exists({"_id": file_id})
        >>> fs.exists(_id=file_id)

        As are the following two calls:

        >>> fs.exists({"filename": "mike.txt"})
        >>> fs.exists(filename="mike.txt")

        And the following two:

        >>> fs.exists({"foo": {"$gt": 12}})
        >>> fs.exists(foo={"$gt": 12})

        Returns ``True`` if a matching file exists, ``False``
        otherwise. Calls to :meth:`exists` will not automatically
        create appropriate indexes; application developers should be
        sure to create indexes if needed and as appropriate.

        :Parameters:
          - `document_or_id` (optional): query document, or _id of the
            document to check for
          - `**kwargs` (optional): keyword arguments are used as a
            query document, if they're present.
        """
        if kwargs:
            return self.__files.find_one(kwargs, ["_id"]) is not None
        return self.__files.find_one(document_or_id, ["_id"]) is not None
