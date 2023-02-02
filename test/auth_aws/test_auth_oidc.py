# Copyright 2020-present MongoDB, Inc.
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

"""Test MONGODB-OIDC Authentication."""

import os
import sys
import unittest

sys.path[0:0] = [""]

from pymongo import MongoClient


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri = os.environ["MONGODB_URI"]

    def test_connect_environment_var(self):
        aws_token_dir = os.environ["AWS_TOKEN_DIR"]

        def get_auth_token(info, timeout):
            with open(os.path.join(aws_token_dir, "test_user1_expires")) as fid:
                token = fid.read()
            return dict(access_token=token)

        def refresh_auth_token(server_info, auth_info, timeout):
            with open(os.path.join(aws_token_dir, "test_user1")) as fid:
                token = fid.read()
            return dict(access_token=token)

        props = dict(
            on_oidc_request_token=get_auth_token,
            on_oidc_refresh_token=refresh_auth_token,
            principal_name="test_user1",
        )
        client = MongoClient(self.uri, authmechanismproperties=props)
        client.test.test.find_one()

        import time

        time.sleep(60)

        orders = client.test.orders
        inventory = client.test.inventory
        with client.start_session() as session:
            with session.start_transaction():
                orders.insert_one({"sku": "abc123", "qty": 100}, session=session)
                inventory.update_one(
                    {"sku": "abc123", "qty": {"$gte": 100}},
                    {"$inc": {"qty": -100}},
                    session=session,
                )


if __name__ == "__main__":
    unittest.main()
