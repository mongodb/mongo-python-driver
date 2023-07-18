# Copyright 2023-present MongoDB, Inc.
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

"""Run the auth spec tests."""
from __future__ import annotations

import os
import sys
import unittest
from typing import Any, Mapping

sys.path[0:0] = [""]

from test import IntegrationTest, client_context, unittest
from test.unified_format import generate_test_classes

from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "index_management")


class TestCreateSearchIndex(IntegrationTest):
    @client_context.require_version_min(7, 0, -1)
    @client_context.require_no_serverless
    def test_inputs(self):
        coll = self.client.test.test
        coll.drop()
        definition = dict(mappings=dict(dynamic=True))
        model_kwarg_list: list[Mapping[str, Any]] = [
            dict(definition=definition, name=None),
            dict(definition=definition, name="test"),
        ]
        for model_kwargs in model_kwarg_list:
            model = SearchIndexModel(**model_kwargs)
            with self.assertRaises(OperationFailure):
                coll.create_search_index(model)
            with self.assertRaises(OperationFailure):
                coll.create_search_index(model_kwargs)


globals().update(
    generate_test_classes(
        _TEST_PATH,
        module=__name__,
    )
)

if __name__ == "__main__":
    unittest.main()
