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

"""Test GridFSBucket class."""
import copy
import datetime
import os
import sys
import re

from json import loads

import gridfs

sys.path[0:0] = [""]

from bson import Binary
from bson.int64 import Int64
from bson.json_util import object_hook
from bson.py3compat import bytes_from_hex
from gridfs.errors import NoFile, CorruptGridFile
from test import (unittest,
                  IntegrationTest)

# Commands.
_COMMANDS = {"delete": lambda coll, doc: [coll.delete_many(d["q"])
                                          for d in doc['deletes']],
             "insert": lambda coll, doc: coll.insert_many(doc['documents']),
             "update": lambda coll, doc: [coll.update_many(u["q"], u["u"])
                                          for u in doc['updates']]
            }

# Location of JSON test specifications.
_TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'gridfs')


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case. Special case for _id.
    if camel == "id":
        return "file_id"
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


class TestAllScenarios(IntegrationTest):
    @classmethod
    def setUpClass(cls):
        super(TestAllScenarios, cls).setUpClass()
        cls.fs = gridfs.GridFSBucket(cls.db)
        cls.str_to_cmd = {
            "upload": cls.fs.upload_from_stream,
            "download": cls.fs.open_download_stream,
            "delete": cls.fs.delete,
            "download_by_name": cls.fs.open_download_stream_by_name}

    def init_db(self, data, test):
        self.db.drop_collection("fs.files")
        self.db.drop_collection("fs.chunks")
        self.db.drop_collection("expected.files")
        self.db.drop_collection("expected.chunks")

        # Read in data.
        if data['files']:
            self.db.fs.files.insert_many(data['files'])
            self.db.expected.files.insert_many(data['files'])
        if data['chunks']:
            self.db.fs.chunks.insert_many(data['chunks'])
            self.db.expected.chunks.insert_many(data['chunks'])

        # Make initial modifications.
        if "arrange" in test:
            for cmd in test['arrange'].get('data', []):
                for key in cmd.keys():
                    if key in _COMMANDS:
                        coll = self.db.get_collection(cmd[key])
                        _COMMANDS[key](coll, cmd)

    def init_expected_db(self, test, result):
        # Modify outcome DB.
        for cmd in test['assert'].get('data', []):
            for key in cmd.keys():
                if key in _COMMANDS:
                    # Replace wildcards in inserts.
                    for doc in cmd.get('documents', []):
                        keylist = doc.keys()
                        for dockey in copy.deepcopy(list(keylist)):
                            if "result" in str(doc[dockey]):
                                doc[dockey] = result
                            if "actual" in str(doc[dockey]):  # Avoid duplicate
                                doc.pop(dockey)
                            # Move contentType to metadata.
                            if dockey == "contentType":
                                doc["metadata"] = {dockey: doc.pop(dockey)}
                    coll = self.db.get_collection(cmd[key])
                    _COMMANDS[key](coll, cmd)

        if test['assert'].get('result') == "&result":
            test['assert']['result'] = result

    def sorted_list(self, coll, ignore_id):
        to_sort = []
        for doc in coll.find():
            docstr = "{"
            if ignore_id:  # Cannot compare _id in chunks collection.
                doc.pop("_id")
            for k in sorted(doc.keys()):
                if k == "uploadDate":  # Can't compare datetime.
                    self.assertTrue(isinstance(doc[k], datetime.datetime))
                else:
                    docstr += "%s:%s " % (k, repr(doc[k]))
            to_sort.append(docstr + "}")
        return to_sort


def create_test(scenario_def):
    def run_scenario(self):

        # Run tests.
        self.assertTrue(scenario_def['tests'], "tests cannot be empty")
        for test in scenario_def['tests']:
            self.init_db(scenario_def['data'], test)

            # Run GridFs Operation.
            operation = self.str_to_cmd[test['act']['operation']]
            args = test['act']['arguments']
            extra_opts = args.pop("options", {})
            if "contentType" in extra_opts:
                extra_opts["metadata"] = {
                    "contentType": extra_opts.pop("contentType")}

            args.update(extra_opts)

            converted_args = dict((camel_to_snake(c), v)
                                  for c, v in args.items())

            expect_error = test['assert'].get("error", False)
            result = None
            error = None
            try:
                result = operation(**converted_args)

                if 'download' in test['act']['operation']:
                    result = Binary(result.read())
            except Exception as exc:
                if not expect_error:
                    raise
                error = exc

            self.init_expected_db(test, result)

            # Asserts.
            errors = {"FileNotFound": NoFile,
                      "ChunkIsMissing": CorruptGridFile,
                      "ExtraChunk": CorruptGridFile,
                      "ChunkIsWrongSize": CorruptGridFile,
                      "RevisionNotFound": NoFile}

            if expect_error:
                self.assertIsNotNone(error)
                self.assertIsInstance(error, errors[test['assert']['error']],
                                      test['description'])
            else:
                self.assertIsNone(error)

            if 'result' in test['assert']:
                if test['assert']['result'] == 'void':
                    test['assert']['result'] = None
                self.assertEqual(result, test['assert'].get('result'))

            if 'data' in test['assert']:
                # Create alphabetized list
                self.assertEqual(
                    set(self.sorted_list(self.db.fs.chunks, True)),
                    set(self.sorted_list(self.db.expected.chunks, True)))

                self.assertEqual(
                    set(self.sorted_list(self.db.fs.files, False)),
                    set(self.sorted_list(self.db.expected.files, False)))

    return run_scenario

def _object_hook(dct):
    if 'length' in dct:
        dct['length'] = Int64(dct['length'])
    return object_hook(dct)

def create_tests():
    for dirpath, _, filenames in os.walk(_TEST_PATH):
        for filename in filenames:
            with open(os.path.join(dirpath, filename)) as scenario_stream:
                scenario_def = loads(
                    scenario_stream.read(), object_hook=_object_hook)

            # Because object_hook is already defined by bson.json_util,
            # and everything is named 'data'
            def str2hex(jsn):
                for key, val in jsn.items():
                    if key in ("data", "source", "result"):
                        if "$hex" in val:
                            jsn[key] = Binary(bytes_from_hex(val['$hex']))
                    if isinstance(jsn[key], dict):
                        str2hex(jsn[key])
                    if isinstance(jsn[key], list):
                        for k in jsn[key]:
                            str2hex(k)

            str2hex(scenario_def)

            # Construct test from scenario.
            new_test = create_test(scenario_def)
            test_name = 'test_%s' % (
                os.path.splitext(filename)[0])
            new_test.__name__ = test_name
            setattr(TestAllScenarios, new_test.__name__, new_test)


create_tests()

if __name__ == "__main__":
    unittest.main()
