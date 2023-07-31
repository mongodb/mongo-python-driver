from __future__ import annotations

from typing import Any, Generic, Optional

from greenletio import async_

from bson import CodecOptions
from pymongo import WriteConcern, common
from pymongo.asyncio.cursor import AsyncCursor
from pymongo.asyncio.database import Database
from pymongo.client_session import ClientSession
from pymongo.collection import Collection as _Collection
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.typings import _DocumentType, _DocumentTypeArg


class Collection(common.BaseObject, Generic[_DocumentType]):
    def __init__(
        self,
        database: Database[_DocumentType],
        name: str,
        create: Optional[bool] = False,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> None:
        self.collection = _Collection(
            database,
            name,
            create,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
            session,
            **kwargs,
        )

    async def async_find(self, *args: Any, **kwargs: Any) -> AsyncCursor:
        sync_cursor = await async_(self.collection.find)(*args, **kwargs)
        return AsyncCursor(sync_cursor)
