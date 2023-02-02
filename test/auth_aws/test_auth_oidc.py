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
from typing import Dict

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.auth import _oidc_cache


class TestAuthOIDC(unittest.TestCase):
    uri: str

    @classmethod
    def setUpClass(cls):
        cls.uri_single = os.environ["MONGODB_URI_SINGLE"]
        cls.uri_multiple = os.environ["MONGODB_URI_MULTIPLE"]
        cls.token_dir = os.environ["AWS_TOKEN_DIR"]

    def test_connect_aws_device_workflow(self):
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user1")
        props = dict(DEVICE_NAME="aws")
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"] = os.path.join(self.token_dir, "test_user2")
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_connect_authorization_code_workflow(self):
        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token)

        props: Dict = dict(on_oidc_request_token=request_token)
        client = MongoClient(self.uri_single, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        _oidc_cache.clear()
        props["PRINCIPAL_NAME"] = "test_user1"
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

        _oidc_cache.clear()
        props["PRINCIPAL_NAME"] = "test_user2"
        token_file = os.path.join(self.token_dir, "test_user2")
        client = MongoClient(self.uri_multiple, authmechanismproperties=props)
        client.test.test.find_one()
        client.close()

    def test_bad_callbacks(self):
        _oidc_cache.clear()

        def request_token_null(info, timeout):
            return None

        props: Dict = dict(on_oidc_request_token=request_token_null)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_token_no_token(info, timeout):
            return dict()

        _oidc_cache.clear()
        props: Dict = dict(on_oidc_request_token=request_token_no_token)
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_refresh_null(info, creds, timeout):
            return None

        token_file = os.path.join(self.token_dir, "test_user1")

        def request_token(info, timeout):
            with open(token_file) as fid:
                token = fid.read()
            return dict(access_token=token)

        _oidc_cache.clear()
        props: Dict = dict(
            on_oidc_request_token=request_token, on_oidc_refresh_token=request_refresh_null
        )
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()

        def request_refresh_no_token(info, creds, timeout):
            return dict()

        _oidc_cache.clear()
        props["on_oidc_refresh_token"] = request_refresh_no_token
        client = MongoClient(self.uri_single, authMechanismProperties=props)
        client.test.test.find_one()
        client.close()

        client = MongoClient(self.uri_single, authMechanismProperties=props)
        with self.assertRaises(ValueError):
            client.test.test.find_one()
        client.close()


if __name__ == "__main__":
    unittest.main()
