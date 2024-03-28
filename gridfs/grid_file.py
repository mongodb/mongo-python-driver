# Copyright 2009-present MongoDB, Inc.
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
from __future__ import annotations

import datetime
import io
import math
import os
from typing import Any, Iterable, Mapping, NoReturn, Optional

from bson.int64 import Int64
from bson.objectid import ObjectId
from gridfs.errors import CorruptGridFile, FileExists, NoFile
from pymongo import ASCENDING
from pymongo.asynchronous import delegate_method, delegate_property, synchronize
from pymongo.client_session import ClientSession
from pymongo.collection import AsyncCollection, Collection
from pymongo.common import MAX_MESSAGE_SIZE
from pymongo.cursor import AsyncCursor, Cursor
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    CursorNotFound,
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
)
from pymongo.helpers import _check_write_command_response
from pymongo.read_preferences import ReadPreference

_SEEK_SET = os.SEEK_SET
_SEEK_CUR = os.SEEK_CUR
_SEEK_END = os.SEEK_END

EMPTY = b""
NEWLN = b"\n"

"""Default chunk size, in bytes."""
# Slightly under a power of 2, to work well with server's record allocations.
DEFAULT_CHUNK_SIZE = 255 * 1024
# The number of chunked bytes to buffer before calling insert_many.
_UPLOAD_BUFFER_SIZE = MAX_MESSAGE_SIZE
# The number of chunk documents to buffer before calling insert_many.
_UPLOAD_BUFFER_CHUNKS = 100000
# Rough BSON overhead of a chunk document not including the chunk data itself.
# Essentially len(encode({"_id": ObjectId(), "files_id": ObjectId(), "n": 1, "data": ""}))
_CHUNK_OVERHEAD = 60

_C_INDEX: dict[str, Any] = {"files_id": ASCENDING, "n": ASCENDING}
_F_INDEX: dict[str, Any] = {"filename": ASCENDING, "uploadDate": ASCENDING}


def _grid_in_property(
    field_name: str,
    docstring: str,
    read_only: Optional[bool] = False,
    closed_only: Optional[bool] = False,
) -> Any:
    """Create a GridIn property."""

    def getter(self: Any) -> Any:
        if closed_only and not self._closed:
            raise AttributeError("can only get %r on a closed file" % field_name)
        # Protect against PHP-237
        if field_name == "length":
            return self._file.get(field_name, 0)
        return self._file.get(field_name, None)

    def setter(self: Any, value: Any) -> Any:
        if self._closed:
            self._coll.files.update_one({"_id": self._file["_id"]}, {"$set": {field_name: value}})
        self._file[field_name] = value

    if read_only:
        docstring += "\n\nThis attribute is read-only."
    elif closed_only:
        docstring = "{}\n\n{}".format(
            docstring,
            "This attribute is read-only and "
            "can only be read after :meth:`close` "
            "has been called.",
        )

    if not read_only and not closed_only:
        return property(getter, setter, doc=docstring)
    return property(getter, doc=docstring)


def _grid_out_property(field_name: str, docstring: str) -> Any:
    """Create a GridOut property."""

    def getter(self: Any) -> Any:
        self._ensure_file()

        # Protect against PHP-237
        if field_name == "length":
            return self._file.get(field_name, 0)
        return self._file.get(field_name, None)

    docstring += "\n\nThis attribute is read-only."
    return property(getter, doc=docstring)


def _clear_entity_type_registry(entity: Any, **kwargs: Any) -> Any:
    """Clear the given database/collection object's type registry."""
    codecopts = entity.codec_options.with_options(type_registry=None)
    return entity.with_options(codec_options=codecopts, **kwargs)


def _disallow_transactions(session: Optional[ClientSession]) -> None:
    if session and session.in_transaction:
        raise InvalidOperation("GridFS does not support multi-document transactions")


class AsyncGridIn:
    """Class to write data to GridFS."""

    def __init__(
        self,
        root_collection: AsyncCollection,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> None:
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
            chunks, in bytes (default: 255 kb)

          - ``"encoding"``: encoding used for this file. Any :class:`str`
            that is written to the file will be converted to :class:`bytes`.

        :param root_collection: root collection to write to
        :param session: a
            :class:`~pymongo.client_session.ClientSession` to use for all
            commands
        :param kwargs: Any: file level options (see above)

        .. versionchanged:: 4.0
           Removed the `disable_md5` parameter. See
           :ref:`removed-gridfs-checksum` for details.

        .. versionchanged:: 3.7
           Added the `disable_md5` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.0
           `root_collection` must use an acknowledged
           :attr:`~pymongo.collection.Collection.write_concern`
        """
        if not isinstance(root_collection, AsyncCollection):
            raise TypeError("root_collection must be an instance of AsyncCollection")

        if not root_collection.write_concern.acknowledged:
            raise ConfigurationError("root_collection must use acknowledged write_concern")
        _disallow_transactions(session)

        # Handle alternative naming
        if "content_type" in kwargs:
            kwargs["contentType"] = kwargs.pop("content_type")
        if "chunk_size" in kwargs:
            kwargs["chunkSize"] = kwargs.pop("chunk_size")

        coll = _clear_entity_type_registry(root_collection, read_preference=ReadPreference.PRIMARY)

        # Defaults
        kwargs["_id"] = kwargs.get("_id", ObjectId())
        kwargs["chunkSize"] = kwargs.get("chunkSize", DEFAULT_CHUNK_SIZE)
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_coll", coll)
        object.__setattr__(self, "_chunks", coll.chunks)
        object.__setattr__(self, "_file", kwargs)
        object.__setattr__(self, "_buffer", io.BytesIO())
        object.__setattr__(self, "_position", 0)
        object.__setattr__(self, "_chunk_number", 0)
        object.__setattr__(self, "_closed", False)
        object.__setattr__(self, "_ensured_index", False)
        object.__setattr__(self, "_buffered_docs", [])
        object.__setattr__(self, "_buffered_docs_size", 0)

    async def _create_index(
        self, collection: AsyncCollection, index_key: Any, unique: bool
    ) -> None:
        doc = await collection.find_one(projection={"_id": 1}, session=self._session)
        if doc is None:
            try:
                index_keys = [
                    index_spec["key"]
                    async for index_spec in await collection.list_indexes(session=self._session)
                ]
            except OperationFailure:
                index_keys = []
            if index_key not in index_keys:
                await collection.create_index(
                    index_key.items(), unique=unique, session=self._session
                )

    async def _ensure_indexes(self) -> None:
        if not object.__getattribute__(self, "_ensured_index"):
            _disallow_transactions(self._session)
            await self._create_index(self._coll.files, _F_INDEX, False)
            await self._create_index(self._coll.chunks, _C_INDEX, True)
            object.__setattr__(self, "_ensured_index", True)

    async def abort(self) -> None:
        """Remove all chunks/files that may have been uploaded and close."""
        await self._coll.chunks.delete_many({"files_id": self._file["_id"]}, session=self._session)
        await self._coll.files.delete_one({"_id": self._file["_id"]}, session=self._session)
        object.__setattr__(self, "_closed", True)

    @property
    def closed(self) -> bool:
        """Is this file closed?"""
        return self._closed

    _id: Any = _grid_in_property("_id", "The ``'_id'`` value for this file.", read_only=True)
    filename: Optional[str] = _grid_in_property("filename", "Name of this file.")
    name: Optional[str] = _grid_in_property("filename", "Alias for `filename`.")
    content_type: Optional[str] = _grid_in_property(
        "contentType", "DEPRECATED, will be removed in PyMongo 5.0. Mime-type for this file."
    )
    length: int = _grid_in_property("length", "Length (in bytes) of this file.", closed_only=True)
    chunk_size: int = _grid_in_property("chunkSize", "Chunk size for this file.", read_only=True)
    upload_date: datetime.datetime = _grid_in_property(
        "uploadDate", "Date that this file was uploaded.", closed_only=True
    )
    md5: Optional[str] = _grid_in_property(
        "md5",
        "DEPRECATED, will be removed in PyMongo 5.0. MD5 of the contents of this file if an md5 sum was created.",
        closed_only=True,
    )

    _buffer: io.BytesIO
    _closed: bool
    _buffered_docs: list[dict[str, Any]]
    _buffered_docs_size: int

    def __getattr__(self, name: str) -> Any:
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridIn object has no attribute '%s'" % name)

    async def set(self, name: str, value: Any) -> None:
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
                await self._coll.files.update_one(
                    {"_id": self._file["_id"]}, {"$set": {name: value}}
                )

    async def _flush_data(self, data: Any, force: bool = False) -> None:
        """Flush `data` to a chunk."""
        await self._ensure_indexes()
        assert len(data) <= self.chunk_size
        if data:
            self._buffered_docs.append(
                {"files_id": self._file["_id"], "n": self._chunk_number, "data": data}
            )
            self._buffered_docs_size += len(data) + _CHUNK_OVERHEAD
        if not self._buffered_docs:
            return
        # Limit to 100,000 chunks or 32MB (+1 chunk) of data.
        if (
            force
            or self._buffered_docs_size >= _UPLOAD_BUFFER_SIZE
            or len(self._buffered_docs) >= _UPLOAD_BUFFER_CHUNKS
        ):
            try:
                await self._chunks.insert_many(self._buffered_docs, session=self._session)
            except BulkWriteError as exc:
                # For backwards compatibility, raise an insert_one style exception.
                write_errors = exc.details["writeErrors"]
                for err in write_errors:
                    if err.get("code") in (11000, 11001, 12582):  # Duplicate key errors
                        self._raise_file_exists(self._file["_id"])
                result = {"writeErrors": write_errors}
                wces = exc.details["writeConcernErrors"]
                if wces:
                    result["writeConcernError"] = wces[-1]
                _check_write_command_response(result)
                raise
            self._buffered_docs = []
            self._buffered_docs_size = 0
        self._chunk_number += 1
        self._position += len(data)

    async def _flush_buffer(self, force: bool = False) -> None:
        """Flush the buffer contents out to a chunk."""
        await self._flush_data(self._buffer.getvalue(), force=force)
        self._buffer.close()
        self._buffer = io.BytesIO()

    async def _flush(self) -> Any:
        """Flush the file to the database."""
        try:
            await self._flush_buffer(force=True)
            # The GridFS spec says length SHOULD be an Int64.
            self._file["length"] = Int64(self._position)
            self._file["uploadDate"] = datetime.datetime.now(tz=datetime.timezone.utc)

            return await self._coll.files.insert_one(self._file, session=self._session)
        except DuplicateKeyError:
            self._raise_file_exists(self._id)

    def _raise_file_exists(self, file_id: Any) -> NoReturn:
        """Raise a FileExists exception for the given file_id."""
        raise FileExists("file with _id %r already exists" % file_id)

    async def close(self) -> None:
        """Flush the file and close it.

        A closed file cannot be written any more. Calling
        :meth:`close` more than once is allowed.
        """
        if not self._closed:
            await self._flush()
            object.__setattr__(self, "_closed", True)

    def read(self, size: int = -1) -> NoReturn:
        raise io.UnsupportedOperation("read")

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    async def write(self, data: Any) -> None:
        """Write data to the file. There is no return value.

        `data` can be either a string of bytes or a file-like object
        (implementing :meth:`read`). If the file has an
        :attr:`encoding` attribute, `data` can also be a
        :class:`str` instance, which will be encoded as
        :attr:`encoding` before being written.

        Due to buffering, the data may not actually be written to the
        database until the :meth:`close` method is called. Raises
        :class:`ValueError` if this file is already closed. Raises
        :class:`TypeError` if `data` is not an instance of
        :class:`bytes`, a file-like object, or an instance of :class:`str`.
        Unicode data is only allowed if the file has an :attr:`encoding`
        attribute.

        :param data: string of bytes or file-like object to be written
            to the file
        """
        if self._closed:
            raise ValueError("cannot write to a closed file")

        try:
            # file-like
            read = data.read
        except AttributeError:
            # string
            if not isinstance(data, (str, bytes)):
                raise TypeError("can only write strings or file-like objects") from None
            if isinstance(data, str):
                try:
                    data = data.encode(self.encoding)
                except AttributeError:
                    raise TypeError(
                        "must specify an encoding for file in order to write str"
                    ) from None
            read = io.BytesIO(data).read

        if self._buffer.tell() > 0:
            # Make sure to flush only when _buffer is complete
            space = self.chunk_size - self._buffer.tell()
            if space:
                try:
                    to_write = read(space)
                except BaseException:
                    await self.abort()
                    raise
                self._buffer.write(to_write)
                if len(to_write) < space:
                    return  # EOF or incomplete
            await self._flush_buffer()
        to_write = read(self.chunk_size)
        while to_write and len(to_write) == self.chunk_size:
            await self._flush_data(to_write)
            to_write = read(self.chunk_size)
        self._buffer.write(to_write)

    async def writelines(self, sequence: Iterable[Any]) -> None:
        """Write a sequence of strings to the file.

        Does not add separators.
        """
        for line in sequence:
            await self.write(line)

    def writeable(self) -> bool:
        return True

    async def __aenter__(self) -> AsyncGridIn:
        """Support for the context manager protocol."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        """Support for the context manager protocol.

        Close the file if no exceptions occur and allow exceptions to propagate.
        """
        if exc_type is None:
            # No exceptions happened.
            await self.close()
        else:
            # Something happened, at minimum mark as closed.
            object.__setattr__(self, "_closed", True)

        # propagate exceptions
        return False


class GridIn:
    """Class to write data to GridFS."""

    def __init__(
        self, root_collection: Collection, session: Optional[ClientSession] = None, **kwargs: Any
    ) -> None:
        self._delegate = AsyncGridIn(root_collection._delegate, session, **kwargs)
        self._coll = Collection.wrap(self._delegate._coll)

    @synchronize()
    def abort(self) -> None:
        ...

    @delegate_property()
    def closed(self) -> bool:
        ...

    _id: Any = _grid_in_property("_id", "The ``'_id'`` value for this file.", read_only=True)
    filename: Optional[str] = _grid_in_property("filename", "Name of this file.")
    name: Optional[str] = _grid_in_property("filename", "Alias for `filename`.")
    content_type: Optional[str] = _grid_in_property(
        "contentType", "DEPRECATED, will be removed in PyMongo 5.0. Mime-type for this file."
    )
    length: int = _grid_in_property("length", "Length (in bytes) of this file.", closed_only=True)
    chunk_size: int = _grid_in_property("chunkSize", "Chunk size for this file.", read_only=True)
    upload_date: datetime.datetime = _grid_in_property(
        "uploadDate", "Date that this file was uploaded.", closed_only=True
    )
    md5: Optional[str] = _grid_in_property(
        "md5",
        "DEPRECATED, will be removed in PyMongo 5.0. MD5 of the contents of this file if an md5 sum was created.",
        closed_only=True,
    )

    _buffer: io.BytesIO
    _closed: bool
    _buffered_docs: list[dict[str, Any]]
    _buffered_docs_size: int

    def __getattr__(self, name: str) -> Any:
        try:
            return self._delegate.__getattr__(name)
        except AttributeError as e:
            raise AttributeError("GridIn object has no attribute '%s'" % name) from e

    def __setattr__(self, name: str, value: Any) -> None:
        # For properties of this instance like _buffer, or descriptors set on
        # the class like filename, use regular __setattr__
        if name in self._delegate.__dict__ or name in self._delegate.__class__.__dict__:
            object.__setattr__(self._delegate, name, value)
        else:
            # All other attributes are part of the document in db.fs.files.
            # Store them to be sent to server on close() or if closed, send
            # them now.
            self._delegate._file[name] = value
            if self._delegate._closed:
                self._coll.files.update_one({"_id": self._file["_id"]}, {"$set": {name: value}})

    @synchronize()
    def close(self) -> None:
        ...

    @delegate_method()
    def read(self, size: int = -1) -> NoReturn:
        ...

    @delegate_method()
    def readable(self) -> bool:
        ...

    @delegate_method()
    def seekable(self) -> bool:
        ...

    @synchronize()
    def write(self, data: Any) -> None:
        ...

    @synchronize()
    def writelines(self, sequence: Iterable[Any]) -> None:
        ...

    @delegate_method()
    def writeable(self) -> bool:
        ...

    def __enter__(self) -> GridIn:
        """Support for the context manager protocol."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        """Support for the context manager protocol.

        Close the file if no exceptions occur and allow exceptions to propagate.
        """
        if exc_type is None:
            # No exceptions happened.
            self.close()
        else:
            # Something happened, at minimum mark as closed.
            object.__setattr__(self._delegate, "_closed", True)

        # propagate exceptions
        return False


class AsyncGridOut(io.IOBase):
    """Class to read data out of GridFS."""

    def __init__(
        self,
        root_collection: AsyncCollection,
        file_id: Optional[int] = None,
        file_document: Optional[Any] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Read a file from GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Either `file_id` or `file_document` must be specified,
        `file_document` will be given priority if present. Raises
        :class:`TypeError` if `root_collection` is not an instance of
        :class:`~pymongo.collection.Collection`.

        :param root_collection: root collection to read from
        :param file_id: value of ``"_id"`` for the file to read
        :param file_document: file document from
            `root_collection.files`
        :param session: a
            :class:`~pymongo.client_session.ClientSession` to use for all
            commands

        .. versionchanged:: 3.8
           For better performance and to better follow the GridFS spec,
           :class:`GridOut` now uses a single cursor to read all the chunks in
           the file.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.0
           Creating a GridOut does not immediately retrieve the file metadata
           from the server. Metadata is fetched when first needed.
        """
        if not isinstance(root_collection, AsyncCollection):
            raise TypeError("root_collection must be an instance of AsyncCollection")
        _disallow_transactions(session)

        root_collection = _clear_entity_type_registry(root_collection)

        super().__init__()

        self._chunks = root_collection.chunks
        self._files = root_collection.files
        self._file_id = file_id
        self._buffer = EMPTY
        # Start position within the current buffered chunk.
        self._buffer_pos = 0
        self._chunk_iter = None
        # Position within the total file.
        self._position = 0
        self._file = file_document
        self._session = session

    _id: Any = _grid_out_property("_id", "The ``'_id'`` value for this file.")
    filename: str = _grid_out_property("filename", "Name of this file.")
    name: str = _grid_out_property("filename", "Alias for `filename`.")
    content_type: Optional[str] = _grid_out_property(
        "contentType", "DEPRECATED, will be removed in PyMongo 5.0. Mime-type for this file."
    )
    length: int = _grid_out_property("length", "Length (in bytes) of this file.")
    chunk_size: int = _grid_out_property("chunkSize", "Chunk size for this file.")
    upload_date: datetime.datetime = _grid_out_property(
        "uploadDate", "Date that this file was first uploaded."
    )
    aliases: Optional[list[str]] = _grid_out_property(
        "aliases", "DEPRECATED, will be removed in PyMongo 5.0. List of aliases for this file."
    )
    metadata: Optional[Mapping[str, Any]] = _grid_out_property(
        "metadata", "Metadata attached to this file."
    )
    md5: Optional[str] = _grid_out_property(
        "md5",
        "DEPRECATED, will be removed in PyMongo 5.0. MD5 of the contents of this file if an md5 sum was created.",
    )

    _file: Any
    _chunk_iter: Any

    async def open(self) -> None:
        _disallow_transactions(self._session)
        self._file = await self._files.find_one({"_id": self._file_id}, session=self._session)
        if not self._file:
            raise NoFile(f"no file in gridfs collection {self._files!r} with _id {self._file_id!r}")

    def __getattr__(self, name: str) -> Any:
        if not self._file:
            raise InvalidOperation(
                "You must call AsyncGridOut.open() before accessing the %s property" % name
            )
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridOut object has no attribute '%s'" % name)

    def readable(self) -> bool:
        return True

    async def readchunk(self) -> bytes:
        """Reads a chunk at a time. If the current position is within a
        chunk the remainder of the chunk is returned.
        """
        received = len(self._buffer) - self._buffer_pos
        chunk_data = EMPTY
        chunk_size = int(self.chunk_size)

        if received > 0:
            chunk_data = self._buffer[self._buffer_pos :]
        elif self._position < int(self.length):
            chunk_number = int((received + self._position) / chunk_size)
            if self._chunk_iter is None:
                self._chunk_iter = _AsyncGridOutChunkIterator(
                    self, self._chunks, self._session, chunk_number
                )

            chunk = await self._chunk_iter.next()
            chunk_data = chunk["data"][self._position % chunk_size :]

            if not chunk_data:
                raise CorruptGridFile("truncated chunk")

        self._position += len(chunk_data)
        self._buffer = EMPTY
        self._buffer_pos = 0
        return chunk_data

    async def _read_size_or_line(self, size: int = -1, line: bool = False) -> bytes:
        """Internal read() and readline() helper."""
        await self.open()
        remainder = int(self.length) - self._position
        if size < 0 or size > remainder:
            size = remainder

        if size == 0:
            return EMPTY

        received = 0
        data = []
        while received < size:
            needed = size - received
            if self._buffer:
                # Optimization: Read the buffer with zero byte copies.
                buf = self._buffer
                chunk_start = self._buffer_pos
                chunk_data = memoryview(buf)[self._buffer_pos :]
                self._buffer = EMPTY
                self._buffer_pos = 0
                self._position += len(chunk_data)
            else:
                buf = self.readchunk()
                chunk_start = 0
                chunk_data = memoryview(buf)
            if line:
                pos = buf.find(NEWLN, chunk_start, chunk_start + needed) - chunk_start
                if pos >= 0:
                    # Decrease size to exit the loop.
                    size = received + pos + 1
                    needed = pos + 1
            if len(chunk_data) > needed:
                data.append(chunk_data[:needed])
                # Optimization: Save the buffer with zero byte copies.
                self._buffer = buf
                self._buffer_pos = chunk_start + needed
                self._position -= len(self._buffer) - self._buffer_pos
            else:
                data.append(chunk_data)
            received += len(chunk_data)

        # Detect extra chunks after reading the entire file.
        if size == remainder and self._chunk_iter:
            try:
                await self._chunk_iter.next()
            except StopIteration:
                pass

        return b"".join(data)

    async def read(self, size: int = -1) -> bytes:
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`bytes`
        If `size` is negative or omitted all data is read.

        :param size: the number of bytes to read

        .. versionchanged:: 3.8
           This method now only checks for extra chunks after reading the
           entire file. Previously, this method would check for extra chunks
           on every call.
        """
        return await self._read_size_or_line(size=size)

    async def readline(self, size: int = -1) -> bytes:  # type: ignore[override]
        """Read one line or up to `size` bytes from the file.

        :param size: the maximum number of bytes to read
        """
        return await self._read_size_or_line(size=size, line=True)

    def tell(self) -> int:
        """Return the current position of this file."""
        return self._position

    async def seek(self, pos: int, whence: int = _SEEK_SET) -> int:
        """Set the current position of this file.

        :param pos: the position (or offset if using relative
           positioning) to seek to
        :param whence: where to seek
           from. :attr:`os.SEEK_SET` (``0``) for absolute file
           positioning, :attr:`os.SEEK_CUR` (``1``) to seek relative
           to the current position, :attr:`os.SEEK_END` (``2``) to
           seek relative to the file's end.

        .. versionchanged:: 4.1
           The method now returns the new position in the file, to
           conform to the behavior of :meth:`io.IOBase.seek`.
        """
        if whence == _SEEK_SET:
            new_pos = pos
        elif whence == _SEEK_CUR:
            new_pos = self._position + pos
        elif whence == _SEEK_END:
            new_pos = int(self.length) + pos
        else:
            raise OSError(22, "Invalid value for `whence`")

        if new_pos < 0:
            raise OSError(22, "Invalid value for `pos` - must be positive")

        # Optimization, continue using the same buffer and chunk iterator.
        if new_pos == self._position:
            return new_pos

        self._position = new_pos
        self._buffer = EMPTY
        self._buffer_pos = 0
        if self._chunk_iter:
            await self._chunk_iter.close()
            self._chunk_iter = None
        return new_pos

    def seekable(self) -> bool:
        return True

    def __aiter__(self) -> AsyncGridOut:
        """Return an iterator over all of this file's data.

        The iterator will return lines (delimited by ``b'\\n'``) of
        :class:`bytes`. This can be useful when serving files
        using a webserver that handles such an iterator efficiently.

        .. versionchanged:: 3.8
           The iterator now raises :class:`CorruptGridFile` when encountering
           any truncated, missing, or extra chunk in a file. The previous
           behavior was to only raise :class:`CorruptGridFile` on a missing
           chunk.

        .. versionchanged:: 4.0
           The iterator now iterates over *lines* in the file, instead
           of chunks, to conform to the base class :py:class:`io.IOBase`.
           Use :meth:`GridOut.readchunk` to read chunk by chunk instead
           of line by line.
        """
        return self

    async def close(self) -> None:
        """Make GridOut more generically file-like."""
        if self._chunk_iter:
            await self._chunk_iter.close()
            self._chunk_iter = None
        super().close()

    def write(self, value: Any) -> NoReturn:
        raise io.UnsupportedOperation("write")

    def writelines(self, lines: Any) -> NoReturn:
        raise io.UnsupportedOperation("writelines")

    def writable(self) -> bool:
        return False

    async def __aenter__(self) -> AsyncGridOut:
        """Makes it possible to use :class:`AsyncGridOut` files
        with the async context manager protocol.
        """
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        """Makes it possible to use :class:`AsyncGridOut` files
        with the async context manager protocol.
        """
        await self.close()
        return False

    def fileno(self) -> NoReturn:
        raise io.UnsupportedOperation("fileno")

    def flush(self) -> None:
        # GridOut is read-only, so flush does nothing.
        pass

    def isatty(self) -> bool:
        return False

    def truncate(self, size: Optional[int] = None) -> NoReturn:
        # See https://docs.python.org/3/library/io.html#io.IOBase.writable
        # for why truncate has to raise.
        raise io.UnsupportedOperation("truncate")

    # Override IOBase.__del__ otherwise it will lead to __getattr__ on
    # __IOBase_closed which calls _ensure_file and potentially performs I/O.
    # We cannot do I/O in __del__ since it can lead to a deadlock.
    def __del__(self) -> None:
        pass


class GridOut(io.IOBase):
    """Class to read data out of GridFS."""

    def __init__(
        self,
        root_collection: Collection,
        file_id: Optional[int] = None,
        file_document: Optional[Any] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        self._delegate = AsyncGridOut(root_collection._delegate, file_id, file_document, session)
        self._coll = Collection.wrap(self._delegate._coll)

    @synchronize()
    def _open(self) -> None:
        ...

    def __getattr__(self, name: str) -> Any:
        if not self._file:
            self._open()
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridOut object has no attribute '%s'" % name)

    @synchronize()
    def readable(self) -> bool:
        ...

    @delegate_method()
    def readchunk(self) -> bytes:
        ...

    @synchronize()
    def read(self, size: int = -1) -> bytes:
        ...

    @delegate_method()
    def readline(self, size: int = -1) -> bytes:  # type: ignore[override]
        ...

    @synchronize()
    def tell(self) -> int:
        ...

    @synchronize()
    def seek(self, pos: int, whence: int = _SEEK_SET) -> int:
        ...

    @delegate_method()
    def seekable(self) -> bool:
        ...

    def __iter__(self) -> GridOut:
        """Return an iterator over all of this file's data.

        The iterator will return lines (delimited by ``b'\\n'``) of
        :class:`bytes`. This can be useful when serving files
        using a webserver that handles such an iterator efficiently.

        .. versionchanged:: 3.8
           The iterator now raises :class:`CorruptGridFile` when encountering
           any truncated, missing, or extra chunk in a file. The previous
           behavior was to only raise :class:`CorruptGridFile` on a missing
           chunk.

        .. versionchanged:: 4.0
           The iterator now iterates over *lines* in the file, instead
           of chunks, to conform to the base class :py:class:`io.IOBase`.
           Use :meth:`GridOut.readchunk` to read chunk by chunk instead
           of line by line.
        """
        return self

    @synchronize()
    def close(self) -> None:
        ...

    @delegate_method()
    def write(self, value: Any) -> NoReturn:
        ...

    @delegate_method()
    def writelines(self, lines: Any) -> NoReturn:
        ...

    @delegate_method()
    def writable(self) -> bool:
        ...

    def __enter__(self) -> GridOut:
        """Makes it possible to use :class:`AsyncGridOut` files
        with the async context manager protocol.
        """
        return self

    @synchronize(async_method_name="__aexit__")
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        ...

    @delegate_method()
    def fileno(self) -> NoReturn:
        ...

    @delegate_method()
    def flush(self) -> None:
        ...

    @delegate_method()
    def isatty(self) -> bool:
        ...

    @delegate_method()
    def truncate(self, size: Optional[int] = None) -> NoReturn:
        ...

    @delegate_method()
    def __del__(self) -> None:
        ...


class _AsyncGridOutChunkIterator:
    """Iterates over a file's chunks using a single cursor.

    Raises CorruptGridFile when encountering any truncated, missing, or extra
    chunk in a file.
    """

    def __init__(
        self,
        grid_out: AsyncGridOut,
        chunks: AsyncCollection,
        session: Optional[ClientSession],
        next_chunk: Any,
    ) -> None:
        self._id = grid_out._id
        self._chunk_size = int(grid_out.chunk_size)
        self._length = int(grid_out.length)
        self._chunks = chunks
        self._session = session
        self._next_chunk = next_chunk
        self._num_chunks = math.ceil(float(self._length) / self._chunk_size)
        self._cursor = None

    _cursor: Optional[AsyncCursor]

    def expected_chunk_length(self, chunk_n: int) -> int:
        if chunk_n < self._num_chunks - 1:
            return self._chunk_size
        return self._length - (self._chunk_size * (self._num_chunks - 1))

    def __aiter__(self) -> _AsyncGridOutChunkIterator:
        return self

    async def _create_cursor(self) -> None:
        filter = {"files_id": self._id}
        if self._next_chunk > 0:
            filter["n"] = {"$gte": self._next_chunk}
        _disallow_transactions(self._session)
        self._cursor = await self._chunks.find(filter, sort=[("n", 1)], session=self._session)

    async def _next_with_retry(self) -> Mapping[str, Any]:
        """Return the next chunk and retry once on CursorNotFound.

        We retry on CursorNotFound to maintain backwards compatibility in
        cases where two calls to read occur more than 10 minutes apart (the
        server's default cursor timeout).
        """
        if self._cursor is None:
            await self._create_cursor()
            assert self._cursor is not None
        try:
            return await self._cursor.next()
        except CursorNotFound:
            await self._cursor.close()
            await self._create_cursor()
            return self._cursor.next()

    async def next(self) -> Mapping[str, Any]:
        try:
            chunk = await self._next_with_retry()
        except StopIteration:
            if self._next_chunk >= self._num_chunks:
                raise
            raise CorruptGridFile("no chunk #%d" % self._next_chunk) from None

        if chunk["n"] != self._next_chunk:
            await self.close()
            raise CorruptGridFile(
                "Missing chunk: expected chunk #%d but found "
                "chunk with n=%d" % (self._next_chunk, chunk["n"])
            )

        if chunk["n"] >= self._num_chunks:
            # According to spec, ignore extra chunks if they are empty.
            if len(chunk["data"]):
                await self.close()
                raise CorruptGridFile(
                    "Extra chunk found: expected %d chunks but found "
                    "chunk with n=%d" % (self._num_chunks, chunk["n"])
                )

        expected_length = self.expected_chunk_length(chunk["n"])
        if len(chunk["data"]) != expected_length:
            await self.close()
            raise CorruptGridFile(
                "truncated chunk #%d: expected chunk length to be %d but "
                "found chunk with length %d" % (chunk["n"], expected_length, len(chunk["data"]))
            )

        self._next_chunk += 1
        return chunk

    __anext__ = next

    async def close(self) -> None:
        if self._cursor:
            await self._cursor.close()
            self._cursor = None


class AsyncGridOutIterator:
    def __init__(self, grid_out: AsyncGridOut, chunks: AsyncCollection, session: ClientSession):
        self._chunk_iter = _AsyncGridOutChunkIterator(grid_out, chunks, session, 0)

    def __aiter__(self) -> AsyncGridOutIterator:
        return self

    async def next(self) -> bytes:
        chunk = await self._chunk_iter.next()
        return bytes(chunk["data"])

    __anext__ = next


class GridOutIterator:
    def __init__(self, grid_out: GridOut, chunks: Collection, session: ClientSession):
        self._delegate = _AsyncGridOutChunkIterator(
            grid_out._delegate, chunks._delegate, session, 0
        )

    def __iter__(self) -> GridOutIterator:
        return self

    @synchronize()
    def next(self) -> bytes:
        ...

    __next__ = next


class AsyncGridOutCursor(AsyncCursor):
    """A cursor / iterator for returning GridOut objects as the result
    of an arbitrary query against the GridFS files collection.
    """

    def __init__(
        self,
        collection: AsyncCollection,
        filter: Optional[Mapping[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        no_cursor_timeout: bool = False,
        sort: Optional[Any] = None,
        batch_size: int = 0,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Create a new cursor, similar to the normal
        :class:`~pymongo.cursor.Cursor`.

        Should not be called directly by application developers - see
        the :class:`~gridfs.GridFS` method :meth:`~gridfs.GridFS.find` instead.

        .. versionadded 2.7

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        _disallow_transactions(session)
        collection = _clear_entity_type_registry(collection)

        # Hold on to the base "fs" collection to create GridOut objects later.
        self._root_collection = collection

        super().__init__(
            collection.files,
            filter,
            skip=skip,
            limit=limit,
            no_cursor_timeout=no_cursor_timeout,
            sort=sort,
            batch_size=batch_size,
            session=session,
        )

    async def next(self) -> AsyncGridOut:
        """Get next GridOut object from cursor."""
        _disallow_transactions(self.session)
        next_file = await super().next()
        return AsyncGridOut(self._root_collection, file_document=next_file, session=self.session)

    __anext__ = next

    def add_option(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Method does not exist for GridOutCursor")

    def remove_option(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Method does not exist for GridOutCursor")

    def _clone_base(self, session: Optional[ClientSession]) -> AsyncGridOutCursor:
        """Creates an empty GridOutCursor for information to be copied into."""
        return AsyncGridOutCursor(self._root_collection, session=session)


class GridOutCursor(Cursor):
    """A cursor / iterator for returning GridOut objects as the result
    of an arbitrary query against the GridFS files collection.
    """

    def __init__(
        self,
        collection: Collection,
        filter: Optional[Mapping[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        no_cursor_timeout: bool = False,
        sort: Optional[Any] = None,
        batch_size: int = 0,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Create a new cursor, similar to the normal
        :class:`~pymongo.cursor.Cursor`.

        Should not be called directly by application developers - see
        the :class:`~gridfs.GridFS` method :meth:`~gridfs.GridFS.find` instead.

        .. versionadded 2.7

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        self._delegate = AsyncGridOutCursor(
            collection._delegate, filter, skip, limit, no_cursor_timeout, sort, batch_size, session
        )

    @synchronize(wrapper_class=GridOut)
    async def next(self) -> GridOut:
        ...

    __next__ = next

    def add_option(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Method does not exist for GridOutCursor")

    def remove_option(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("Method does not exist for GridOutCursor")
