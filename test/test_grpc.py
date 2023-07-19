# Copyright 2022-present MongoDB, Inc.
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

import sys
import unittest

import grpc

from bson import CodecOptions, decode
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS
from pymongo.errors import AutoReconnect
from pymongo.message import _op_msg
from pymongo.network import receive_message
from pymongo.pool import Pool, PoolOptions

sys.path[0:0] = [""]


class TestGRPC(unittest.TestCase):
    REQUEST = iter(
        [
            b"4\x00\x00\x00*\x0c\xb5:\x00\x00\x00\x00\xdd\x07\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x10hello\x00\x01\x00\x00\x00\x02$db\x00\x06\x00\x00\x00admin\x00\x00"
        ]
    )

    def test_receive_message(self):
        channel = grpc.insecure_channel(
            "host9.local.10gen.cc:9901", options=[("grpc.default_authority", "host.local.10gen.cc")]
        )
        response = channel.stream_stream(
            "/mongodb.CommandService/UnauthenticatedCommandStream"
        ).__call__(
            TestGRPC.REQUEST,
            metadata=[
                ("security-uuid", "uuid"),
                ("username", "user"),
                ("servername", "host.local.10gen.cc"),
                ("mongodb-wireversion", "18"),
                ("x-forwarded-for", "127.0.0.1:9901"),
            ],
        )

        for msg in response:
            unpacked = receive_message(msg, 984943658)
            processed = unpacked.command_response(CodecOptions())
            self.assertEqual(processed["ok"], 1)

    def test_create_pool(self):
        test_pool = Pool(
            ("host9.local.10gen.cc", 9901),
            PoolOptions(max_pool_size=1, connect_timeout=1, socket_timeout=1, wait_queue_timeout=1),
        )
        test_pool.ready()

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of AutoReconnect.
        with test_pool.get_socket() as sock:
            response = sock.connector.stream_stream(
                "/mongodb.CommandService/UnauthenticatedCommandStream"
            ).__call__(
                TestGRPC.REQUEST,
                metadata=[
                    ("security-uuid", "uuid"),
                    ("username", "user"),
                    ("servername", "host.local.10gen.cc"),
                    ("mongodb-wireversion", "18"),
                    ("x-forwarded-for", "127.0.0.1:9901"),
                ],
            )

            for msg in response:
                unpacked = receive_message(msg, 984943658)
                processed = unpacked.command_response(CodecOptions())
                self.assertEqual(processed["ok"], 1)

    def test_commands(self):
        test_pool = Pool(
            ("host9.local.10gen.cc", 9901),
            PoolOptions(max_pool_size=1, connect_timeout=1, socket_timeout=1, wait_queue_timeout=1),
        )
        test_pool.ready()

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of AutoReconnect.
        with test_pool.get_socket() as sock:
            response1 = sock.command("$db", {"hello": 1})
            response2 = sock.command("$db", {"buildInfo": 1})
            self.assertEqual(response1["ok"], 1)
            self.assertEqual(response2["ok"], 1)


if __name__ == "__main__":
    unittest.main()
