from __future__ import annotations

from typing import Any, Dict, Generic, MutableMapping, Optional, Sequence, Union

from greenletio import async_

import bson
from pymongo import MongoClient, WriteConcern, common
from pymongo.client_session import ClientSession
from pymongo.database import Database as _Database
from pymongo.database import _CodecDocumentType
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode
from pymongo.typings import _DocumentType, _DocumentTypeArg


class Database(common.BaseObject, Generic[_DocumentType]):
    def __init__(
        self,
        client: MongoClient[_DocumentType],
        name: str,
        codec_options: Optional[bson.CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> None:
        self.database = _Database(
            client, name, codec_options, read_preference, write_concern, read_concern
        )

    async def async_command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        value: Any = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: Optional[_ServerMode] = None,
        codec_options: Optional[bson.codec_options.CodecOptions[_CodecDocumentType]] = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> Union[None, None, Union[Dict[str, Any], _CodecDocumentType]]:
        return await async_(self.database.command)(
            command,
            value,
            check,
            allowable_errors,
            read_preference,
            codec_options,
            session,
            comment,
            **kwargs,
        )
