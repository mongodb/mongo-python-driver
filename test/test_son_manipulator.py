# Copyright 2009-2015 MongoDB, Inc.
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

"""Tests for SONManipulators.
"""

import sys
sys.path[0:0] = [""]

from bson.errors import InvalidDocument
from bson.son import SON
from pymongo import MongoClient
from pymongo.son_manipulator import (DeterministicKeyEscaper,
                                     NamespaceInjector,
                                     NonDeterministicKeyEscaper,
                                     ObjectIdInjector,
                                     ObjectIdShuffler,
                                     SONManipulator)
from test import client_context, qcheck, unittest


class TestSONManipulator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(
            client_context.host, client_context.port, connect=False)
        cls.db = client.pymongo_test

    def test_basic(self):
        manip = SONManipulator()
        collection = self.db.test

        def incoming_is_identity(son):
            return son == manip.transform_incoming(son, collection)
        qcheck.check_unittest(self, incoming_is_identity,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_id_injection(self):
        manip = ObjectIdInjector()
        collection = self.db.test

        def incoming_adds_id(son):
            son = manip.transform_incoming(son, collection)
            assert "_id" in son
            return True
        qcheck.check_unittest(self, incoming_adds_id,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_id_shuffling(self):
        manip = ObjectIdShuffler()
        collection = self.db.test

        def incoming_moves_id(son_in):
            son = manip.transform_incoming(son_in, collection)
            if not "_id" in son:
                return True
            for (k, v) in son.items():
                self.assertEqual(k, "_id")
                break
            # Key order matters in SON equality test,
            # matching collections.OrderedDict
            if isinstance(son_in, SON):
                return son_in.to_dict() == son.to_dict()
            return son_in == son

        self.assertTrue(incoming_moves_id({}))
        self.assertTrue(incoming_moves_id({"_id": 12}))
        self.assertTrue(incoming_moves_id({"hello": "world", "_id": 12}))
        self.assertTrue(incoming_moves_id(SON([("hello", "world"),
                                               ("_id", 12)])))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))

    def test_ns_injection(self):
        manip = NamespaceInjector()
        collection = self.db.test

        def incoming_adds_ns(son):
            son = manip.transform_incoming(son, collection)
            assert "_ns" in son
            return son["_ns"] == collection.name
        qcheck.check_unittest(self, incoming_adds_ns,
                              qcheck.gen_mongo_dict(3))

        def outgoing_is_identity(son):
            return son == manip.transform_outgoing(son, collection)
        qcheck.check_unittest(self, outgoing_is_identity,
                              qcheck.gen_mongo_dict(3))


class TestKeyEscaper(unittest.TestCase):
    """
    Defines base functionality and templates for KeyEscaper test cases.
    """
    @classmethod
    def setUpClass(cls):
        client = MongoClient(
            client_context.host, client_context.port, connect=False)
        cls.collection = client.pymongo_test.test

    def _store_document(self, document):
        """Stores a document to MongoDB and returns its _id value."""
        escaped = \
            self.manipulator.transform_incoming(document, self.collection.name)
        return self.collection.insert_one(escaped).inserted_id


    def _retrieve_document(self, filter_):
        """Retrieves a document from MongoDB."""
        # Load the stored document from the database, omitting the '_id'
        #   field, since we can't predict that value for the comparison.
        stored = self.collection.find_one(
            filter=filter_,
            projection={'_id': False},
        )

        unescaped = \
            self.manipulator.transform_outgoing(stored, self.collection.name)

        if isinstance(unescaped, SON):
            unescaped = unescaped.to_dict()

        return unescaped


    def assertKeysEscaped(self, document):
        """
        Asserts that the KeyEscaper correctly escapes/unescapes keys in the
          document.
        """
        # Store the document to MongoDB and retrieve it again, to ensure
        #   that the KeyEscaper made the document storable.
        document_id = self._store_document(document)
        unescaped = self._retrieve_document({'_id': document_id})

        self.assertEqual(unescaped, document)


class TestDeterministicKeyEscaper(TestKeyEscaper):
    def setUp(self):
        self.manipulator = DeterministicKeyEscaper()

    def test_illegal_key_names_dollar(self):
        """
        The stored document includes keys that starts with '$'.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            '$topLevel': {
                'severity': 'innocent enough',
                'explanation': 'this is a common enough use case',
            },

            'nested': {
                '$tricky': 'can we handle nested values?',

                '$deep': {
                    '$reference': 'we need to go deeper',
                },
            },

            '$iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'perfectly$legal':
                "keys may contain '$' so long as it's not the first character",

            'string': '$values may start with "$", no problem',
            'list': ['$list', '$items', '$are', '$also', '$exempt'],
        })


    def test_illegal_key_names_dot(self):
        """
        The stored document includes keys that include '.' characters.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            'top.level': {
                'severity': 'innocent enough',
                'explanation': 'this is a common enough use case',
            },

            'nested': {
                '.tricky': 'can we handle nested values?',

                '.deep': {
                    'reference.': 'we need to go deeper',
                },
            },

            '.iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'string': 'values.may.contain "." no.problem',
            'list': ['.list', 'items.', '.are.', '.also', 'exempt.'],
        })


    def test_illegal_key_names_null(self):
        """
        The stored document includes keys that include null bytes.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            # These all should evaluate to the same code point, but
            #   just to make absolutely sure....
            '\U00000000top\x00level\u0000': {
                'severity': 'suspect',

                'explanation':
                    'not sure why you would ever really need to do this',
            },

            'nested': {
                '\x00tricky\u0000': 'can we handle nested values?',

                '\x00deep': {
                    '\U00000000reference': 'we need to go deeper',
                },
            },

            '\x00iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'string': 'values\x00may\x00contain\x00nulls\x00no\x00problem',

            'list': [
                '\x00list',
                'items\x00',
                '\x00are\x00',
                'also\x00',
                '\x00exempt',
            ],
        })


    def test_illegal_key_names_magic(self):
        """
        The stored document includes key names that coincide with
            escaped keys.
        """
        self.assertKeysEscaped({
            # This is the attribute where the self.manipulator stores the
            #   escaped keys.
            self.manipulator.magic_prefix: {
                'severity': 'strange',
                'explanation': 'i guess it could happen',
            },

            # This is an example of an escaped key.
            self.manipulator.magic_prefix + '1':
                'somebody has to think of these things',

            # This is nonsense, but props for creative thinking.
            self.manipulator.magic_prefix + 'wonka':
                'there is no life i know to compare with pure imagination',

            'nested': {
                self.manipulator.magic_prefix: 'can we handle nested values?',
                self.manipulator.magic_prefix + '0': 'same story, different day',

                self.manipulator.magic_prefix + 'deep': {
                    self.manipulator.magic_prefix: 'we need to go deeper',
                },
            },

            self.manipulator.magic_prefix + 'iñtërnâtiônàlizætiøn':
                'non-ascii characters supported, too',

            # Values may use the magic prefix without consequence.
            'string': self.manipulator.magic_prefix,

            'list': [
                self.manipulator.magic_prefix,
                self.manipulator.magic_prefix + '0',
                self.manipulator.magic_prefix + 'foobar',
            ],
        })


    def test_illegal_key_names_combo(self):
        """The stored document has all kinds of illegal keys."""
        self.assertKeysEscaped({
            self.manipulator.magic_prefix + '$very.very.\x00illegal\x00': {
                'severity': 'major',
                'explanation': 'did you even read the instructions?',
            },

            'nested': {
                '$dollars': 'starts with $',
                'has.dot': 'contains a .',
                'has\x00null': 'contains a null',
                '$iñtërnâtiônàlizætiøn': 'contains non-ascii',

                '$level.down': {
                    '..': 'low-budget ascii bear',
                },

                self.manipulator.magic_prefix: 'overslept',
            },
        })


    def test_safe_byte_strings(self):
        """
        Byte strings are allowed, so long as they can be converted into
          unicode strings.
        """
        document_id = self._store_document({
            b'$ascii_escaped': 'escaped, safe; contains ascii only',
            b'ascii_unescaped': 'unescaped, safe; contains ascii only',

            '$iñtërnâtiônàlizætiøn_escaped'.encode('utf-8'):
                'escaped, safe; non-ascii, but can be decoded w/ default encoding',

            'iñtërnâtiônàlizætiøn_unescaped'.encode('utf-8'):
                'unescaped, safe; non-ascii, but can be decoded w/ default encoding',
        })

        retrieved = self._retrieve_document({'_id': document_id})

        self.assertDictEqual(
            retrieved,

            {
                # Note that keys are automatically converted to unicode strings
                #   before storage.
                '$ascii_escaped': 'escaped, safe; contains ascii only',
                'ascii_unescaped': 'unescaped, safe; contains ascii only',

                '$iñtërnâtiônàlizætiøn_escaped':
                    'escaped, safe; non-ascii, but can be decoded w/ default encoding',

                'iñtërnâtiônàlizætiøn_unescaped':
                    'unescaped, safe; non-ascii, but can be decoded w/ default encoding',
            },
        )


    def test_unsafe_byte_strings(self):
        """
        Any byte string that can't be converted into a unicode string is
          invalid.
        """
        # Ensure that we pick the wrong encoding, regardless of system
        #   configuration.
        wrong_encoding = \
            'latin-1' if sys.getdefaultencoding() == 'utf-16' else 'utf-16'

        with self.assertRaises(InvalidDocument):
            self.manipulator.transform_incoming(
                {'$iñtërnâtiônàlizætiøn'.encode(wrong_encoding): 'wrong encoding!'},
                self.collection.name,
            )

        # An exception will be raised even if the key doesn't need to be
        #   escaped.
        with self.assertRaises(InvalidDocument):
            self.manipulator.transform_incoming(
                {'iñtërnâtiônàlizætiøn'.encode(wrong_encoding): 'wrong encoding!'},
                self.collection.name,
            )

    def test_query_by_escaped_key(self):
        """
        It is possible (with a little work) to find a document using an
          escaped key.
        """
        document = {
            'data': {
                'responseValues': {
                    '$firstName': 'Marcus',
                    '$lastName': 'Brody',
                },
            },
        }

        self._store_document(document)

        # If we escape the entire search key, we won't find anything,
        #   because the entire thing will be escaped.
        self.assertIsNone(
            self.collection.find_one({
                self.manipulator.escape_key(
                    'data.responseValues.$lastName'): 'Brody',
            })
        )

        # Instead, we need to escape just the final part of the filter key.
        self.assertDictEqual(
            self._retrieve_document({
                'data.responseValues.' + self.manipulator.escape_key(
                    '$lastName'):
                    'Brody',
            }),

            document,
        )


class TestNonDeterministicKeyEscaper(TestKeyEscaper):
    def setUp(self):
        self.manipulator = NonDeterministicKeyEscaper()

    def test_illegal_key_names_dollar(self):
        """
        The stored document includes keys that starts with '$'.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            '$topLevel': {
                'severity': 'innocent enough',
                'explanation': 'this is a common enough use case',
            },

            'nested': {
                '$tricky': 'can we handle nested values?',

                '$deep': {
                    '$reference': 'we need to go deeper',
                },
            },

            '$iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'perfectly$legal':
                "keys may contain '$' so long as it's not the first character",

            'string': '$values may start with "$", no problem',
            'list': ['$list', '$items', '$are', '$also', '$exempt'],
        })


    def test_illegal_key_names_dot(self):
        """
        The stored document includes keys that include '.' characters.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            'top.level': {
                'severity': 'innocent enough',
                'explanation': 'this is a common enough use case',
            },

            'nested': {
                '.tricky': 'can we handle nested values?',

                '.deep': {
                    'reference.': 'we need to go deeper',
                },
            },

            '.iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'string': 'values.may.contain "." no.problem',
            'list': ['.list', 'items.', '.are.', '.also', 'exempt.'],
        })


    def test_illegal_key_names_null(self):
        """
        The stored document includes keys that include null bytes.

        This is a MongoDB no-no, according to
        https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
        """
        self.assertKeysEscaped({
            # These all should evaluate to the same code point, but
            #   just to make absolutely sure....
            '\U00000000top\x00level\u0000': {
                'severity': 'suspect',

                'explanation':
                    'not sure why you would ever really need to do this',
            },

            'nested': {
                '\x00tricky\u0000': 'can we handle nested values?',

                '\x00deep': {
                    '\U00000000reference': 'we need to go deeper',
                },
            },

            '\x00iñtërnâtiônàlizætiøn': 'non-ascii characters supported, too',

            'string': 'values\x00may\x00contain\x00nulls\x00no\x00problem',

            'list': [
                '\x00list',
                'items\x00',
                '\x00are\x00',
                'also\x00',
                '\x00exempt',
            ],
        })


    def test_illegal_key_names_magic(self):
        """
        The stored document includes key names that coincide with
            escaped keys.
        """
        self.assertKeysEscaped({
            # This is the attribute where the self.manipulator stores the
            #   escaped keys.
            self.manipulator.magic_prefix: {
                'severity': 'strange',
                'explanation': 'i guess it could happen',
            },

            # This is an example of an escaped key.
            self.manipulator.magic_prefix + '1':
                'somebody has to think of these things',

            # This is nonsense, but props for creative thinking.
            self.manipulator.magic_prefix + 'wonka':
                'there is no life i know to compare with pure imagination',

            'nested': {
                self.manipulator.magic_prefix: 'can we handle nested values?',
                self.manipulator.magic_prefix + '0': 'same story, different day',

                self.manipulator.magic_prefix + 'deep': {
                    self.manipulator.magic_prefix: 'we need to go deeper',
                },
            },

            self.manipulator.magic_prefix + 'iñtërnâtiônàlizætiøn':
                'non-ascii characters supported, too',

            # Values may use the magic prefix without consequence.
            'string': self.manipulator.magic_prefix,

            'list': [
                self.manipulator.magic_prefix,
                self.manipulator.magic_prefix + '0',
                self.manipulator.magic_prefix + 'foobar',
            ],
        })


    def test_illegal_key_names_combo(self):
        """The stored document has all kinds of illegal keys."""
        self.assertKeysEscaped({
            self.manipulator.magic_prefix + '$very.very.\x00illegal\x00': {
                'severity': 'major',
                'explanation': 'did you even read the instructions?',
            },

            'nested': {
                '$dollars': 'starts with $',
                'has.dot': 'contains a .',
                'has\x00null': 'contains a null',
                '$iñtërnâtiônàlizætiøn': 'contains non-ascii',

                '$level.down': {
                    '..': 'low-budget ascii bear',
                },

                self.manipulator.magic_prefix: 'overslept',
            },
        })


    def test_safe_byte_strings(self):
        """
        Byte strings are allowed, so long as they can be converted into
          unicode strings.
        """
        document_id = self._store_document({
            b'$ascii_escaped': 'escaped, safe; contains ascii only',
            b'ascii_unescaped': 'unescaped, safe; contains ascii only',

            '$iñtërnâtiônàlizætiøn_escaped'.encode('utf-8'):
                'escaped, safe; non-ascii, but can be decoded w/ default encoding',

            'iñtërnâtiônàlizætiøn_unescaped'.encode('utf-8'):
                'unescaped, safe; non-ascii, but can be decoded w/ default encoding',
        })

        retrieved = self._retrieve_document({'_id': document_id})

        self.assertDictEqual(
            retrieved,

            {
                # Note that keys are automatically converted to unicode strings
                #   before storage.
                '$ascii_escaped': 'escaped, safe; contains ascii only',
                'ascii_unescaped': 'unescaped, safe; contains ascii only',

                '$iñtërnâtiônàlizætiøn_escaped':
                    'escaped, safe; non-ascii, but can be decoded w/ default encoding',

                'iñtërnâtiônàlizætiøn_unescaped':
                    'unescaped, safe; non-ascii, but can be decoded w/ default encoding',
            },
        )


    def test_unsafe_byte_strings(self):
        """
        Any byte string that can't be converted into a unicode string is
          invalid.
        """
        # Ensure that we pick the wrong encoding, regardless of system
        #   configuration.
        wrong_encoding = \
            'latin-1' if sys.getdefaultencoding() == 'utf-16' else 'utf-16'

        with self.assertRaises(InvalidDocument):
            self.manipulator.transform_incoming(
                {'$iñtërnâtiônàlizætiøn'.encode(wrong_encoding): 'wrong encoding!'},
                self.collection.name,
            )

        # An exception will be raised even if the key doesn't need to be
        #   escaped.
        with self.assertRaises(InvalidDocument):
            self.manipulator.transform_incoming(
                {'iñtërnâtiônàlizætiøn'.encode(wrong_encoding): 'wrong encoding!'},
                self.collection.name,
            )

    def test_query_by_escaped_key(self):
        """
        As its name suggests, NonDeterministicKeyEscaper uses (effectively)
          unpredictable replacement names for escaped keys.
        """
        document = {
            'data': {
                'responseValues': {
                    '$firstName': 'Marcus',
                    '$lastName': 'Brody',
                },
            },
        }

        self._store_document(document)

        #
        # It is theoretically possible to guess the correct escaped key,
        #   but outside of contrived examples in unit tests, it's very
        #   unlikely that this approach will ever be practical.
        #
        # If you want to be able to query against escaped keys, you're
        #   better off using DeterministicKeyEscaper.
        #
        self.assertIsNone(
            self.collection.find_one({
                'data.responseValues.' + self.manipulator.escape_key(
                    '$lastName'):
                    'Brody',
            })
        )

if __name__ == "__main__":
    unittest.main()
