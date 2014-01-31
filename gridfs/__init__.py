# Copyright 2009-2014 MongoDB, Inc.
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
                              GridOut,
                              GridOutCursor)
from pymongo import (MongoClient,
                     ASCENDING,
                     DESCENDING)
from pymongo.database import Database


class GridFS(object):
    """An instance of GridFS on top of a single Database.
    """
    def __init__(self, database, collection="fs", _connect=True):
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
        if _connect:
            self.__ensure_index_files_id()

    def __is_secondary(self):
        client = self.__database.connection

        # Connect the client, so we know if it's connected to the primary.
        client._ensure_connected()
        return isinstance(client, MongoClient) and not client.is_primary

    def __ensure_index_files_id(self):
        if not self.__is_secondary():
            self.__chunks.ensure_index([("files_id", ASCENDING),
                                        ("n", ASCENDING)],
                                       unique=True)

    def __ensure_index_filename(self):
        if not self.__is_secondary():
            self.__files.ensure_index([("filename", ASCENDING),
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

        .. versionadded:: 1.6
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

        .. versionadded:: 1.9
           The ability to write :class:`unicode`, if an `encoding` has
           been specified as a keyword argument.

        .. versionadded:: 1.6
        """
        grid_file = GridIn(self.__collection, **kwargs)

        # Start a request - necessary if w=0, harmless otherwise
        request = self.__collection.database.connection.start_request()
        try:
            try:
                grid_file.write(data)
            finally:
                grid_file.close()
        finally:
            # Ensure request is ended even if close() throws error
            request.end()
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

        .. versionchanged:: 1.11
           `filename` defaults to None;
        .. versionadded:: 1.11
           Accept keyword arguments to find files by custom metadata.
        .. versionadded:: 1.9
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
            grid_file = cursor.next()
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

        .. versionchanged:: 1.11
           `filename` defaults to None;
        .. versionadded:: 1.11
           Accept keyword arguments to find files by custom metadata. See
           :meth:`get_version`.
        .. versionadded:: 1.6
        """
        return self.get_version(filename=filename, **kwargs)

    # TODO add optional safe mode for chunk removal?
    def delete(self, file_id):
        """Delete a file from GridFS by ``"_id"``.

        Removes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        .. note:: Deletes of non-existent files are considered successful
           since the end result is the same: no file with that _id remains.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to delete

        .. versionadded:: 1.6
        """
        self.__ensure_index_files_id()
        self.__files.remove({"_id": file_id},
                            **self.__files._get_wc_override())
        self.__chunks.remove({"files_id": file_id})

    def list(self):
        """List the names of all files stored in this instance of
        :class:`GridFS`.

        An index on ``{filename: 1, uploadDate: -1}`` will
        automatically be created when this method is called the first
        time.

        .. versionchanged:: 2.7
           ``list`` ensures an index, the same as ``get_version``.

        .. versionchanged:: 1.6
           Removed the `collection` argument.
        """
        self.__ensure_index_filename()

        # With an index, distinct includes documents with no filename
        # as None.
        return [
            name for name in self.__files.distinct("filename")
            if name is not None]

    def find(self, *args, **kwargs):
        """Query GridFS for files.

        Returns a cursor that iterates across files matching
        arbitrary queries on the files collection. Can be combined
        with other modifiers for additional control. For example::

          for grid_out in fs.find({"filename": "lisa.txt"}, timeout=False):
              data = grid_out.read()

        would iterate through all versions of "lisa.txt" stored in GridFS.
        Note that setting timeout to False may be important to prevent the
        cursor from timing out during long multi-file processing work.

        As another example, the call::

          most_recent_three = fs.find().sort("uploadDate", -1).limit(3)

        would return a cursor to the three most recently uploaded files
        in GridFS.

        Follows a similar interface to
        :meth:`~pymongo.collection.Collection.find`
        in :class:`~pymongo.collection.Collection`.

        :Parameters:
          - `spec` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `skip` (optional): the number of files to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `timeout` (optional): if True (the default), any returned
            cursor is closed by the server after 10 minutes of
            inactivity. If set to False, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with timeout turned off are properly closed.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `max_scan` (optional): limit the number of file documents
            examined when performing the query
          - `read_preference` (optional): The read preference for
            this query.
          - `tag_sets` (optional): The tag sets for this query.
          - `secondary_acceptable_latency_ms` (optional): Any replica-set
            member whose ping time is within secondary_acceptable_latency_ms of
            the nearest member may accept reads. Default 15 milliseconds.
            **Ignored by mongos** and must be configured on the command line.
            See the localThreshold_ option for more information.
          - `compile_re` (optional): if ``False``, don't attempt to compile
            BSON regex objects into Python regexes. Return instances of
            :class:`~bson.regex.Regex` instead.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~gridfs.grid_file.GridOutCursor`
        corresponding to this query.

        .. versionadded:: 2.7
        .. mongodoc:: find
        .. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold
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

        .. versionadded:: 1.8
        """
        if kwargs:
            return self.__files.find_one(kwargs, ["_id"]) is not None
        return self.__files.find_one(document_or_id, ["_id"]) is not None

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
