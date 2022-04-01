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

"""Used by test_client.TestClient.test_sigstop_sigcont."""

import logging
import os
import sys

sys.path[0:0] = [""]

from pymongo import monitoring
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

SERVER_API = None
MONGODB_API_VERSION = os.environ.get("MONGODB_API_VERSION")
if MONGODB_API_VERSION:
    SERVER_API = ServerApi(MONGODB_API_VERSION)


class HeartbeatLogger(monitoring.ServerHeartbeatListener):
    """Log events until the listener is closed."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    def started(self, event: monitoring.ServerHeartbeatStartedEvent) -> None:
        if self.closed:
            return
        logging.info("%s", event)

    def succeeded(self, event: monitoring.ServerHeartbeatSucceededEvent) -> None:
        if self.closed:
            return
        logging.info("%s", event)

    def failed(self, event: monitoring.ServerHeartbeatFailedEvent) -> None:
        if self.closed:
            return
        logging.warning("%s", event)


def main(uri: str) -> None:
    heartbeat_logger = HeartbeatLogger()
    client = MongoClient(
        uri,
        event_listeners=[heartbeat_logger],
        heartbeatFrequencyMS=500,
        connectTimeoutMS=500,
        server_api=SERVER_API,
    )
    client.admin.command("ping")
    logging.info("TEST STARTED")
    # test_sigstop_sigcont will SIGSTOP and SIGCONT this process in this loop.
    while True:
        try:
            data = input('Type "q" to quit: ')
        except EOFError:
            break
        if data == "q":
            break
    client.admin.command("ping")
    logging.info("TEST COMPLETED")
    heartbeat_logger.close()
    client.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("unknown or missing options")
        print(f"usage: python3 {sys.argv[0]} 'mongodb://localhost'")
        exit(1)

    # Enable logs in this format:
    # 2022-03-30 12:40:55,582 INFO <ServerHeartbeatStartedEvent ('localhost', 27017)>
    FORMAT = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    main(sys.argv[1])
