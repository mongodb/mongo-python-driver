# Copyright 2014-present MongoDB, Inc.
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

"""Represent a response from the server."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Mapping, Union

if TYPE_CHECKING:
    from datetime import datetime, timedelta

    from pymongo.message import _OpMsg, _OpReply
    from pymongo.pool import SocketInfo
    from pymongo.typings import _Address


class Response:
    __slots__ = ("_data", "_address", "_request_id", "_duration", "_from_command", "_docs")

    def __init__(
        self,
        data: Union[_OpMsg, _OpReply],
        address: _Address,
        request_id: int,
        duration: Union[timedelta, datetime, None],
        from_command: bool,
        docs: List[Mapping[str, Any]],
    ):
        """Represent a response from the server.

        :Parameters:
          - `data`: A network response message.
          - `address`: (host, port) of the source server.
          - `request_id`: The request id of this operation.
          - `duration`: The duration of the operation.
          - `from_command`: if the response is the result of a db command.
        """
        self._data = data
        self._address = address
        self._request_id = request_id
        self._duration = duration
        self._from_command = from_command
        self._docs = docs

    @property
    def data(self) -> Union[_OpMsg, _OpReply]:
        """Server response's raw BSON bytes."""
        return self._data

    @property
    def address(self) -> _Address:
        """(host, port) of the source server."""
        return self._address

    @property
    def request_id(self) -> int:
        """The request id of this operation."""
        return self._request_id

    @property
    def duration(self) -> Union[timedelta, datetime, None]:
        """The duration of the operation."""
        return self._duration

    @property
    def from_command(self) -> bool:
        """If the response is a result from a db command."""
        return self._from_command

    @property
    def docs(self) -> List[Mapping[str, Any]]:
        """The decoded document(s)."""
        return self._docs


class PinnedResponse(Response):
    __slots__ = ("_socket_info", "_more_to_come")

    def __init__(
        self,
        data: Union[_OpMsg, _OpReply],
        address: _Address,
        socket_info: SocketInfo,
        request_id: int,
        duration: Union[timedelta, datetime, None],
        from_command: bool,
        docs: List[Mapping[str, Any]],
        more_to_come: bool,
    ):
        """Represent a response to an exhaust cursor's initial query.

        :Parameters:
          - `data`:  A network response message.
          - `address`: (host, port) of the source server.
          - `socket_info`: The SocketInfo used for the initial query.
          - `request_id`: The request id of this operation.
          - `duration`: The duration of the operation.
          - `from_command`: If the response is the result of a db command.
          - `docs`: List of documents.
          - `more_to_come`: Bool indicating whether cursor is ready to be
            exhausted.
        """
        super().__init__(data, address, request_id, duration, from_command, docs)
        self._socket_info = socket_info
        self._more_to_come = more_to_come

    @property
    def socket_info(self) -> SocketInfo:
        """The SocketInfo used for the initial query.

        The server will send batches on this socket, without waiting for
        getMores from the client, until the result set is exhausted or there
        is an error.
        """
        return self._socket_info

    @property
    def more_to_come(self) -> bool:
        """If true, server is ready to send batches on the socket until the
        result set is exhausted or there is an error.
        """
        return self._more_to_come
