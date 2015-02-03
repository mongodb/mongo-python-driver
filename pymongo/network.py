# Copyright 2015 MongoDB, Inc.
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

"""Internal network layer helper methods."""

import select
import struct

from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pymongo import helpers, message
from pymongo.errors import AutoReconnect

_UNPACK_INT = struct.Struct("<i").unpack


def command(sock, dbname, spec):
    """Execute a command over the socket, or raise socket.error.

    :Parameters:
      - `sock`: a raw socket object
      - `dbname`: name of the database on which to run the command
      - `spec`: a command document as a dict, SON, or mapping object
    """
    ns = dbname + '.$cmd'
    request_id, msg, _ = message.query(0, ns, 0, -1, spec,
                                       None, DEFAULT_CODEC_OPTIONS)
    sock.sendall(msg)
    response = receive_message(sock, 1, request_id)
    unpacked = helpers._unpack_response(response)['data'][0]
    msg = "command %s on namespace %s failed: %%s" % (
        repr(spec).replace("%", "%%"), ns)
    helpers._check_command_response(unpacked, msg)
    return unpacked


def receive_message(sock, operation, request_id):
    """Receive a raw BSON message or raise socket.error."""
    header = _receive_data_on_socket(sock, 16)
    length = _UNPACK_INT(header[:4])[0]

    # No request_id for exhaust cursor "getMore".
    if request_id is not None:
        response_id = _UNPACK_INT(header[8:12])[0]
        assert request_id == response_id, "ids don't match %r %r" % (
            request_id, response_id)

    assert operation == _UNPACK_INT(header[12:])[0]
    return _receive_data_on_socket(sock, length - 16)


def _receive_data_on_socket(sock, length):
    msg = b""
    while length:
        chunk = sock.recv(length)
        if chunk == b"":
            raise AutoReconnect("connection closed")

        length -= len(chunk)
        msg += chunk

    return msg


def socket_closed(sock):
    """Return True if we know socket has been closed, False otherwise.
    """
    try:
        rd, _, _ = select.select([sock], [], [], 0)
    # Any exception here is equally bad (select.error, ValueError, etc.).
    except:
        return True
    return len(rd) > 0
