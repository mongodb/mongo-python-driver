# Copyright 2009-2010 10gen, Inc.
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

"""Test the connection module."""

import os
import sys
import time
import unittest
import warnings
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.connection import Connection
from pymongo.database import Database
from pymongo.errors import (AutoReconnect,
                            ConnectionFailure,
                            InvalidName,
                            InvalidURI,
                            OperationFailure)
from test import version


def get_connection(*args, **kwargs):
    host = os.environ.get("DB_IP", "localhost")
    port = int(os.environ.get("DB_PORT", 27017))
    return Connection(host, port, *args, **kwargs)


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.host = os.environ.get("DB_IP", "localhost")
        self.port = int(os.environ.get("DB_PORT", 27017))

    def test_types(self):
        self.assertRaises(TypeError, Connection, 1)
        self.assertRaises(TypeError, Connection, 1.14)
        self.assertRaises(TypeError, Connection, [])
        self.assertRaises(TypeError, Connection, "localhost", "27017")
        self.assertRaises(TypeError, Connection, "localhost", 1.14)
        self.assertRaises(TypeError, Connection, "localhost", [])

    def test_constants(self):
        Connection.HOST = self.host
        Connection.PORT = self.port
        self.assert_(Connection())

        Connection.HOST = "somedomainthatdoesntexist.org"
        Connection.PORT = 123456789
        self.assertRaises(ConnectionFailure, Connection)
        self.assert_(Connection(self.host, self.port))

        Connection.HOST = self.host
        Connection.PORT = self.port
        self.assert_(Connection())

    def test_connect(self):
        self.assertRaises(ConnectionFailure, Connection,
                          "somedomainthatdoesntexist.org")
        self.assertRaises(ConnectionFailure, Connection, self.host, 123456789)

        self.assert_(Connection(self.host, self.port))

    def test_repr(self):
        self.assertEqual(repr(Connection(self.host, self.port)),
                         "Connection('%s', %s)" % (self.host, self.port))

    def test_getters(self):
        self.assertEqual(Connection(self.host, self.port).host, self.host)
        self.assertEqual(Connection(self.host, self.port).port, self.port)

    def test_get_db(self):
        connection = Connection(self.host, self.port)

        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, connection, "")
        self.assertRaises(InvalidName, make_db, connection, "te$t")
        self.assertRaises(InvalidName, make_db, connection, "te.t")
        self.assertRaises(InvalidName, make_db, connection, "te\\t")
        self.assertRaises(InvalidName, make_db, connection, "te/t")
        self.assertRaises(InvalidName, make_db, connection, "te st")

        self.assert_(isinstance(connection.test, Database))
        self.assertEqual(connection.test, connection["test"])
        self.assertEqual(connection.test, Database(connection, "test"))

    def test_database_names(self):
        connection = Connection(self.host, self.port)

        connection.pymongo_test.test.save({"dummy": u"object"})
        connection.pymongo_test_mike.test.save({"dummy": u"object"})

        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        self.assert_("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        connection = Connection(self.host, self.port)

        self.assertRaises(TypeError, connection.drop_database, 5)
        self.assertRaises(TypeError, connection.drop_database, None)

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        connection.drop_database("pymongo_test")
        dbs = connection.database_names()
        self.assert_("pymongo_test" not in dbs)

        connection.pymongo_test.test.save({"dummy": u"object"})
        dbs = connection.database_names()
        self.assert_("pymongo_test" in dbs)
        connection.drop_database(connection.pymongo_test)
        dbs = connection.database_names()
        self.assert_("pymongo_test" not in dbs)

    def test_copy_db(self):
        c = Connection(self.host, self.port)

        self.assertRaises(TypeError, c.copy_database, 4, "foo")
        self.assertRaises(TypeError, c.copy_database, "foo", 4)

        self.assertRaises(InvalidName, c.copy_database, "foo", "$foo")

        c.drop_database("pymongo_test")
        c.drop_database("pymongo_test1")
        c.drop_database("pymongo_test2")

        c.pymongo_test.test.insert({"foo": "bar"})

        self.failIf("pymongo_test1" in c.database_names())
        self.failIf("pymongo_test2" in c.database_names())

        c.copy_database("pymongo_test", "pymongo_test1")

        self.assert_("pymongo_test1" in c.database_names())
        self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

        c.copy_database("pymongo_test", "pymongo_test2",
                        "%s:%s" % (self.host, self.port))

        self.assert_("pymongo_test2" in c.database_names())
        self.assertEqual("bar", c.pymongo_test2.test.find_one()["foo"])

        c.drop_database("pymongo_test1")
        c.drop_database("pymongo_test2")

        if version.at_least(c, (1, 3, 3, 1)):
            c.pymongo_test.add_user("mike", "password")

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="foo", password="bar")
            self.failIf("pymongo_test1" in c.database_names())

            self.assertRaises(OperationFailure, c.copy_database,
                              "pymongo_test", "pymongo_test1",
                              username="mike", password="bar")
            self.failIf("pymongo_test1" in c.database_names())

            c.copy_database("pymongo_test", "pymongo_test1",
                            username="mike", password="password")
            self.assert_("pymongo_test1" in c.database_names())
            self.assertEqual("bar", c.pymongo_test1.test.find_one()["foo"])

            c.drop_database("pymongo_test1")

    def test_iteration(self):
        connection = Connection(self.host, self.port)

        def iterate():
            [a for a in connection]

        self.assertRaises(TypeError, iterate)

    # TODO this test is probably very dependent on the machine its running on
    # due to timing issues, but I want to get something in here.
    def test_low_network_timeout(self):
        c = None
        i = 0
        n = 10
        while c is None and i < n:
            try:
                c = Connection(self.host, self.port, network_timeout=0.0001)
            except AutoReconnect:
                i += 1
        if i == n:
            raise SkipTest()

        coll = c.pymongo_test.test

        for _ in range(1000):
            try:
                coll.find_one()
            except AutoReconnect:
                pass
            except AssertionError:
                self.fail()

    # NOTE this probably doesn't all belong in this file, but it's easier in
    # one place
    def test_deprecated_method_for_attr(self):
        c = Connection(self.host, self.port)
        db = c.foo
        coll = db.bar

        warnings.simplefilter("error")

        self.assertRaises(DeprecationWarning, c.host)
        self.assertRaises(DeprecationWarning, c.port)
        self.assertRaises(DeprecationWarning, db.connection)
        self.assertRaises(DeprecationWarning, db.name)
        self.assertRaises(DeprecationWarning, coll.full_name)
        self.assertRaises(DeprecationWarning, coll.name)
        self.assertRaises(DeprecationWarning, coll.database)

        warnings.resetwarnings()
        warnings.simplefilter("ignore")

        self.assertEqual(c.host, c.host())
        self.assertEqual(c.port, c.port())
        self.assertEqual(db.connection, db.connection())
        self.assertEqual(db.name, db.name())
        self.assertEqual(coll.full_name, coll.full_name())
        self.assertEqual(coll.name, coll.name())
        self.assertEqual(coll.database, coll.database())

        warnings.resetwarnings()
        warnings.simplefilter("default")

        self.assertEqual(self.host, c.host)
        self.assertEqual(self.port, c.port)
        self.assertEqual(c, db.connection)
        self.assertEqual("foo", db.name)
        self.assertEqual("foo.bar", coll.full_name)
        self.assertEqual("bar", coll.name)
        self.assertEqual(db, coll.database)

    def test_disconnect(self):
        c = Connection(self.host, self.port)
        coll = c.foo.bar

        c.disconnect()
        c.disconnect()

        coll.count()

        c.disconnect()
        c.disconnect()

        coll.count()

    def test_parse_uri(self):
        self.assertEqual(([("localhost", 27017)], None, None, None),
                         Connection._parse_uri("localhost"))
        self.assertRaises(InvalidURI, Connection._parse_uri, "http://foobar.com")
        self.assertRaises(InvalidURI, Connection._parse_uri, "http://foo@foobar.com")

        self.assertEqual(([("localhost", 27017)], None, None, None),
                         Connection._parse_uri("mongodb://localhost"))
        self.assertEqual(([("localhost", 27017)], None, "fred", "foobar"),
                         Connection._parse_uri("mongodb://fred:foobar@localhost"))
        self.assertEqual(([("localhost", 27017)], "baz", "fred", "foobar"),
                         Connection._parse_uri("mongodb://fred:foobar@localhost/baz"))
        self.assertEqual(([("example1.com", 27017), ("example2.com", 27017)],
                          None, None, None),
                         Connection._parse_uri("mongodb://example1.com:27017,example2.com:27017"))
        self.assertEqual(([("localhost", 27017),
                           ("localhost", 27018),
                           ("localhost", 27019)], None, None, None),
                         Connection._parse_uri("mongodb://localhost,localhost:27018,localhost:27019"))

        self.assertEqual(([("localhost", 27018)], None, None, None),
                         Connection._parse_uri("localhost:27018"))
        self.assertEqual(([("localhost", 27017)], "foo", None, None),
                         Connection._parse_uri("localhost/foo"))
        self.assertEqual(([("localhost", 27017)], None, None, None),
                         Connection._parse_uri("localhost/"))

    def test_from_uri(self):
        c = Connection(self.host, self.port)

        self.assertRaises(InvalidURI, Connection.from_uri, "mongodb://localhost/baz")

        self.assertEqual(c, Connection.from_uri("mongodb://%s:%s" %
                                                (self.host, self.port)))

        c.admin.system.users.remove({})
        c.pymongo_test.system.users.remove({})

        c.admin.add_user("admin", "pass")
        c.pymongo_test.add_user("user", "pass")

        self.assertRaises(InvalidURI, Connection.from_uri,
                          "mongodb://foo:bar@%s:%s" % (self.host, self.port))
        self.assertRaises(InvalidURI, Connection.from_uri,
                          "mongodb://admin:bar@%s:%s" % (self.host, self.port))
        self.assertRaises(InvalidURI, Connection.from_uri,
                          "mongodb://user:pass@%s:%s" % (self.host, self.port))
        Connection.from_uri("mongodb://admin:pass@%s:%s" % (self.host, self.port))

        self.assertRaises(InvalidURI, Connection.from_uri,
                          "mongodb://admin:pass@%s:%s/pymongo_test" %
                          (self.host, self.port))
        self.assertRaises(InvalidURI, Connection.from_uri,
                          "mongodb://user:foo@%s:%s/pymongo_test" %
                          (self.host, self.port))
        Connection.from_uri("mongodb://user:pass@%s:%s/pymongo_test" %
                            (self.host, self.port))

        self.assert_(Connection.from_uri("mongodb://%s:%s" %
                                         (self.host, self.port),
                                         slave_okay=True).slave_okay)

    def test_fork(self):
        """Test using a connection before and after a fork.
        """
        if sys.platform == "win32":
            raise SkipTest()

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest()

        db = Connection(self.host, self.port).pymongo_test

        # Failure occurs if the connection is used before the fork
        db.test.find_one()

        def loop(pipe):
            while True:
                try:
                    db.test.insert({"a": "b"}, safe=True)
                    for _ in db.test.find():
                        pass
                except:
                    pipe.send(True)
                    os._exit(1)

        cp1, cc1 = Pipe()
        cp2, cc2 = Pipe()

        p1 = Process(target=loop, args=(cc1,))
        p2 = Process(target=loop, args=(cc2,))

        p1.start()
        p2.start()

        p1.join(1)
        p2.join(1)

        p1.terminate()
        p2.terminate()

        p1.join()
        p2.join()

        cc1.close()
        cc2.close()

        # recv will only have data if the subprocess failed
        try:
            cp1.recv()
            self.fail()
        except EOFError:
            pass
        try:
            cp2.recv()
            self.fail()
        except EOFError:
            pass

# TODO come up with a different way to test `network_timeout`. This is just
# too sketchy.
#
#     def test_socket_timeout(self):
#         no_timeout = Connection(self.host, self.port)
#         timeout = Connection(self.host, self.port, network_timeout=0.1)

#         no_timeout.pymongo_test.drop_collection("test")

#         no_timeout.pymongo_test.test.save({"x": 1})

#         where_func = """function (doc) {
#   var d = new Date().getTime() + 1000;
#   var x = new Date().getTime();
#   while (x < d) {
#     x = new Date().getTime();
#   }
#   return true;
# }"""

#         def get_x(db):
#             return db.test.find().where(where_func).next()["x"]

#         self.assertEqual(1, get_x(no_timeout.pymongo_test))
#         self.assertRaises(ConnectionFailure, get_x, timeout.pymongo_test)
#         self.assertEqual(1, no_timeout.pymongo_test.test.find().next()["x"])


if __name__ == "__main__":
    unittest.main()
