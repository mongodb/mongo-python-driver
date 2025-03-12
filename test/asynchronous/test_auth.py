# Copyright 2013-present MongoDB, Inc.
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

"""Authentication Tests."""
from __future__ import annotations

import asyncio
import os
import sys
import threading
from urllib.parse import quote_plus

sys.path[0:0] = [""]

from test.asynchronous import (
    AsyncIntegrationTest,
    AsyncPyMongoTestCase,
    SkipTest,
    async_client_context,
    unittest,
)
from test.utils_shared import AllowListEventListener, delay, ignore_deprecations

import pytest

from pymongo import AsyncMongoClient, monitoring
from pymongo.asynchronous.auth import HAVE_KERBEROS, _canonicalize_hostname
from pymongo.auth_shared import _build_credentials_tuple
from pymongo.errors import OperationFailure
from pymongo.hello import HelloCompat
from pymongo.read_preferences import ReadPreference
from pymongo.saslprep import HAVE_STRINGPREP

_IS_SYNC = False

pytestmark = pytest.mark.auth

# YOU MUST RUN KINIT BEFORE RUNNING GSSAPI TESTS ON UNIX.
GSSAPI_HOST = os.environ.get("GSSAPI_HOST")
GSSAPI_PORT = int(os.environ.get("GSSAPI_PORT", "27017"))
GSSAPI_PRINCIPAL = os.environ.get("GSSAPI_PRINCIPAL")
GSSAPI_SERVICE_NAME = os.environ.get("GSSAPI_SERVICE_NAME", "mongodb")
GSSAPI_CANONICALIZE = os.environ.get("GSSAPI_CANONICALIZE", "false")
GSSAPI_SERVICE_REALM = os.environ.get("GSSAPI_SERVICE_REALM")
GSSAPI_PASS = os.environ.get("GSSAPI_PASS")
GSSAPI_DB = os.environ.get("GSSAPI_DB", "test")

SASL_HOST = os.environ.get("SASL_HOST")
SASL_PORT = int(os.environ.get("SASL_PORT", "27017"))
SASL_USER = os.environ.get("SASL_USER")
SASL_PASS = os.environ.get("SASL_PASS")
SASL_DB = os.environ.get("SASL_DB", "$external")


class AutoAuthenticateThread(threading.Thread):
    """Used in testing threaded authentication.

    This does await collection.find_one() with a 1-second delay to ensure it must
    check out and authenticate multiple connections from the pool concurrently.

    :Parameters:
      `collection`: An auth-protected collection containing one document.
    """

    def __init__(self, collection):
        super().__init__()
        self.collection = collection
        self.success = False

    def run(self):
        assert self.collection.find_one({"$where": delay(1)}) is not None
        self.success = True


class TestGSSAPI(AsyncPyMongoTestCase):
    mech_properties: str
    service_realm_required: bool

    @classmethod
    def setUpClass(cls):
        if not HAVE_KERBEROS:
            raise SkipTest("Kerberos module not available.")
        if not GSSAPI_HOST or not GSSAPI_PRINCIPAL:
            raise SkipTest("Must set GSSAPI_HOST and GSSAPI_PRINCIPAL to test GSSAPI")
        cls.service_realm_required = (
            GSSAPI_SERVICE_REALM is not None and GSSAPI_SERVICE_REALM not in GSSAPI_PRINCIPAL
        )
        mech_properties = dict(
            SERVICE_NAME=GSSAPI_SERVICE_NAME, CANONICALIZE_HOST_NAME=GSSAPI_CANONICALIZE
        )
        if GSSAPI_SERVICE_REALM is not None:
            mech_properties["SERVICE_REALM"] = GSSAPI_SERVICE_REALM
        cls.mech_properties = mech_properties

    async def test_credentials_hashing(self):
        # GSSAPI credentials are properly hashed.
        creds0 = _build_credentials_tuple("GSSAPI", None, "user", "pass", {}, None)

        creds1 = _build_credentials_tuple(
            "GSSAPI", None, "user", "pass", {"authmechanismproperties": {"SERVICE_NAME": "A"}}, None
        )

        creds2 = _build_credentials_tuple(
            "GSSAPI", None, "user", "pass", {"authmechanismproperties": {"SERVICE_NAME": "A"}}, None
        )

        creds3 = _build_credentials_tuple(
            "GSSAPI", None, "user", "pass", {"authmechanismproperties": {"SERVICE_NAME": "B"}}, None
        )

        self.assertEqual(1, len({creds1, creds2}))
        self.assertEqual(3, len({creds0, creds1, creds2, creds3}))

    @ignore_deprecations
    async def test_gssapi_simple(self):
        assert GSSAPI_PRINCIPAL is not None
        if GSSAPI_PASS is not None:
            uri = "mongodb://%s:%s@%s:%d/?authMechanism=GSSAPI" % (
                quote_plus(GSSAPI_PRINCIPAL),
                GSSAPI_PASS,
                GSSAPI_HOST,
                GSSAPI_PORT,
            )
        else:
            uri = "mongodb://%s@%s:%d/?authMechanism=GSSAPI" % (
                quote_plus(GSSAPI_PRINCIPAL),
                GSSAPI_HOST,
                GSSAPI_PORT,
            )

        if not self.service_realm_required:
            # Without authMechanismProperties.
            client = self.simple_client(
                GSSAPI_HOST,
                GSSAPI_PORT,
                username=GSSAPI_PRINCIPAL,
                password=GSSAPI_PASS,
                authMechanism="GSSAPI",
            )

            await client[GSSAPI_DB].collection.find_one()

            # Log in using URI, without authMechanismProperties.
            client = self.simple_client(uri)
            await client[GSSAPI_DB].collection.find_one()

        # Authenticate with authMechanismProperties.
        client = self.simple_client(
            GSSAPI_HOST,
            GSSAPI_PORT,
            username=GSSAPI_PRINCIPAL,
            password=GSSAPI_PASS,
            authMechanism="GSSAPI",
            authMechanismProperties=self.mech_properties,
        )

        await client[GSSAPI_DB].collection.find_one()

        # Log in using URI, with authMechanismProperties.
        mech_properties_str = ""
        for key, value in self.mech_properties.items():
            mech_properties_str += f"{key}:{value},"
        mech_uri = uri + f"&authMechanismProperties={mech_properties_str[:-1]}"
        client = self.simple_client(mech_uri)
        await client[GSSAPI_DB].collection.find_one()

        set_name = async_client_context.replica_set_name
        if set_name:
            if not self.service_realm_required:
                # Without authMechanismProperties
                client = self.simple_client(
                    GSSAPI_HOST,
                    GSSAPI_PORT,
                    username=GSSAPI_PRINCIPAL,
                    password=GSSAPI_PASS,
                    authMechanism="GSSAPI",
                    replicaSet=set_name,
                )

                await client[GSSAPI_DB].list_collection_names()

                uri = uri + f"&replicaSet={set_name!s}"
                client = self.simple_client(uri)
                await client[GSSAPI_DB].list_collection_names()

            # With authMechanismProperties
            client = self.simple_client(
                GSSAPI_HOST,
                GSSAPI_PORT,
                username=GSSAPI_PRINCIPAL,
                password=GSSAPI_PASS,
                authMechanism="GSSAPI",
                authMechanismProperties=self.mech_properties,
                replicaSet=set_name,
            )

            await client[GSSAPI_DB].list_collection_names()

            mech_uri = mech_uri + f"&replicaSet={set_name!s}"
            client = self.simple_client(mech_uri)
            await client[GSSAPI_DB].list_collection_names()

    @ignore_deprecations
    @async_client_context.require_sync
    async def test_gssapi_threaded(self):
        client = self.simple_client(
            GSSAPI_HOST,
            GSSAPI_PORT,
            username=GSSAPI_PRINCIPAL,
            password=GSSAPI_PASS,
            authMechanism="GSSAPI",
            authMechanismProperties=self.mech_properties,
        )

        # Authentication succeeded?
        await client.server_info()
        db = client[GSSAPI_DB]

        # Need one document in the collection. AutoAuthenticateThread does
        # collection.find_one with a 1-second delay, forcing it to check out
        # multiple connections from the pool concurrently, proving that
        # auto-authentication works with GSSAPI.
        collection = db.test
        if not await collection.count_documents({}):
            try:
                await collection.drop()
                await collection.insert_one({"_id": 1})
            except OperationFailure:
                raise SkipTest("User must be able to write.")

        threads = []
        for _ in range(4):
            threads.append(AutoAuthenticateThread(collection))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            self.assertTrue(thread.success)

        set_name = async_client_context.replica_set_name
        if set_name:
            client = self.simple_client(
                GSSAPI_HOST,
                GSSAPI_PORT,
                username=GSSAPI_PRINCIPAL,
                password=GSSAPI_PASS,
                authMechanism="GSSAPI",
                authMechanismProperties=self.mech_properties,
                replicaSet=set_name,
            )

            # Succeeded?
            await client.server_info()

            threads = []
            for _ in range(4):
                threads.append(AutoAuthenticateThread(collection))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
                self.assertTrue(thread.success)

    async def test_gssapi_canonicalize_host_name(self):
        # Test the low level method.
        assert GSSAPI_HOST is not None
        result = await _canonicalize_hostname(GSSAPI_HOST, "forward")
        if "compute-1.amazonaws.com" not in result:
            self.assertEqual(result, GSSAPI_HOST)
        result = await _canonicalize_hostname(GSSAPI_HOST, "forwardAndReverse")
        self.assertEqual(result, GSSAPI_HOST)

        # Use the equivalent named CANONICALIZE_HOST_NAME.
        props = self.mech_properties.copy()
        if props["CANONICALIZE_HOST_NAME"] == "true":
            props["CANONICALIZE_HOST_NAME"] = "forwardAndReverse"
        else:
            props["CANONICALIZE_HOST_NAME"] = "none"
        client = self.simple_client(
            GSSAPI_HOST,
            GSSAPI_PORT,
            username=GSSAPI_PRINCIPAL,
            password=GSSAPI_PASS,
            authMechanism="GSSAPI",
            authMechanismProperties=props,
        )
        await client.server_info()

    async def test_gssapi_host_name(self):
        props = self.mech_properties
        props["SERVICE_HOST"] = "example.com"

        # Authenticate with authMechanismProperties.
        client = self.simple_client(
            GSSAPI_HOST,
            GSSAPI_PORT,
            username=GSSAPI_PRINCIPAL,
            password=GSSAPI_PASS,
            authMechanism="GSSAPI",
            authMechanismProperties=self.mech_properties,
        )
        with self.assertRaises(OperationFailure):
            await client.server_info()

        props["SERVICE_HOST"] = GSSAPI_HOST
        client = self.simple_client(
            GSSAPI_HOST,
            GSSAPI_PORT,
            username=GSSAPI_PRINCIPAL,
            password=GSSAPI_PASS,
            authMechanism="GSSAPI",
            authMechanismProperties=self.mech_properties,
        )
        await client.server_info()


class TestSASLPlain(AsyncPyMongoTestCase):
    @classmethod
    def setUpClass(cls):
        if not SASL_HOST or not SASL_USER or not SASL_PASS:
            raise SkipTest("Must set SASL_HOST, SASL_USER, and SASL_PASS to test SASL")

    async def test_sasl_plain(self):
        client = self.simple_client(
            SASL_HOST,
            SASL_PORT,
            username=SASL_USER,
            password=SASL_PASS,
            authSource=SASL_DB,
            authMechanism="PLAIN",
        )
        await client.ldap.test.find_one()

        assert SASL_USER is not None
        assert SASL_PASS is not None
        uri = "mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;authSource=%s" % (
            quote_plus(SASL_USER),
            quote_plus(SASL_PASS),
            SASL_HOST,
            SASL_PORT,
            SASL_DB,
        )
        client = self.simple_client(uri)
        await client.ldap.test.find_one()

        set_name = async_client_context.replica_set_name
        if set_name:
            client = self.simple_client(
                SASL_HOST,
                SASL_PORT,
                replicaSet=set_name,
                username=SASL_USER,
                password=SASL_PASS,
                authSource=SASL_DB,
                authMechanism="PLAIN",
            )
            await client.ldap.test.find_one()

            uri = "mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;authSource=%s;replicaSet=%s" % (
                quote_plus(SASL_USER),
                quote_plus(SASL_PASS),
                SASL_HOST,
                SASL_PORT,
                SASL_DB,
                str(set_name),
            )
            client = self.simple_client(uri)
            await client.ldap.test.find_one()

    async def test_sasl_plain_bad_credentials(self):
        def auth_string(user, password):
            uri = "mongodb://%s:%s@%s:%d/?authMechanism=PLAIN;authSource=%s" % (
                quote_plus(user),
                quote_plus(password),
                SASL_HOST,
                SASL_PORT,
                SASL_DB,
            )
            return uri

        bad_user = self.simple_client(auth_string("not-user", SASL_PASS))
        bad_pwd = self.simple_client(auth_string(SASL_USER, "not-pwd"))
        # OperationFailure raised upon connecting.
        with self.assertRaises(OperationFailure):
            await bad_user.admin.command("ping")
        with self.assertRaises(OperationFailure):
            await bad_pwd.admin.command("ping")


class TestSCRAMSHA1(AsyncIntegrationTest):
    @async_client_context.require_auth
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await async_client_context.create_user(
            "pymongo_test", "user", "pass", roles=["userAdmin", "readWrite"]
        )

    async def asyncTearDown(self):
        await async_client_context.drop_user("pymongo_test", "user")
        await super().asyncTearDown()

    @async_client_context.require_no_fips
    async def test_scram_sha1(self):
        host, port = await async_client_context.host, await async_client_context.port

        client = await self.async_rs_or_single_client_noauth(
            "mongodb://user:pass@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1" % (host, port)
        )
        await client.pymongo_test.command("dbstats")

        if async_client_context.is_rs:
            uri = (
                "mongodb://user:pass"
                "@%s:%d/pymongo_test?authMechanism=SCRAM-SHA-1"
                "&replicaSet=%s" % (host, port, async_client_context.replica_set_name)
            )
            client = await self.async_single_client_noauth(uri)
            await client.pymongo_test.command("dbstats")
            db = client.get_database("pymongo_test", read_preference=ReadPreference.SECONDARY)
            await db.command("dbstats")


# https://github.com/mongodb/specifications/blob/master/source/auth/auth.md#scram-sha-256-and-mechanism-negotiation
class TestSCRAM(AsyncIntegrationTest):
    @async_client_context.require_auth
    @async_client_context.require_version_min(3, 7, 2)
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._SENSITIVE_COMMANDS = monitoring._SENSITIVE_COMMANDS
        monitoring._SENSITIVE_COMMANDS = set()
        self.listener = AllowListEventListener("saslStart")

    async def asyncTearDown(self):
        monitoring._SENSITIVE_COMMANDS = self._SENSITIVE_COMMANDS
        await async_client_context.client.testscram.command("dropAllUsersFromDatabase")
        await async_client_context.client.drop_database("testscram")
        await super().asyncTearDown()

    async def test_scram_skip_empty_exchange(self):
        listener = AllowListEventListener("saslStart", "saslContinue")
        await async_client_context.create_user(
            "testscram", "sha256", "pwd", roles=["dbOwner"], mechanisms=["SCRAM-SHA-256"]
        )

        client = await self.async_rs_or_single_client_noauth(
            username="sha256", password="pwd", authSource="testscram", event_listeners=[listener]
        )
        await client.testscram.command("dbstats")

        if async_client_context.version < (4, 4, -1):
            # Assert we sent the skipEmptyExchange option.
            first_event = listener.started_events[0]
            self.assertEqual(first_event.command_name, "saslStart")
            self.assertEqual(first_event.command["options"], {"skipEmptyExchange": True})

        # Assert the third exchange was skipped on servers that support it.
        # Note that the first exchange occurs on the connection handshake.
        started = listener.started_command_names()
        if async_client_context.version.at_least(4, 4, -1):
            self.assertEqual(started, ["saslContinue"])
        else:
            self.assertEqual(started, ["saslStart", "saslContinue", "saslContinue"])

    @async_client_context.require_no_fips
    async def test_scram(self):
        # Step 1: create users
        await async_client_context.create_user(
            "testscram", "sha1", "pwd", roles=["dbOwner"], mechanisms=["SCRAM-SHA-1"]
        )
        await async_client_context.create_user(
            "testscram", "sha256", "pwd", roles=["dbOwner"], mechanisms=["SCRAM-SHA-256"]
        )
        await async_client_context.create_user(
            "testscram",
            "both",
            "pwd",
            roles=["dbOwner"],
            mechanisms=["SCRAM-SHA-1", "SCRAM-SHA-256"],
        )

        # Step 2: verify auth success cases
        client = await self.async_rs_or_single_client_noauth(
            username="sha1", password="pwd", authSource="testscram"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="sha1", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-1"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="sha256", password="pwd", authSource="testscram"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="sha256", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-256"
        )
        await client.testscram.command("dbstats")

        # Step 2: SCRAM-SHA-1 and SCRAM-SHA-256
        client = await self.async_rs_or_single_client_noauth(
            username="both", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-1"
        )
        await client.testscram.command("dbstats")
        client = await self.async_rs_or_single_client_noauth(
            username="both", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-256"
        )
        await client.testscram.command("dbstats")

        self.listener.reset()
        client = await self.async_rs_or_single_client_noauth(
            username="both", password="pwd", authSource="testscram", event_listeners=[self.listener]
        )
        await client.testscram.command("dbstats")
        if async_client_context.version.at_least(4, 4, -1):
            # Speculative authentication in 4.4+ sends saslStart with the
            # handshake.
            self.assertEqual(self.listener.started_events, [])
        else:
            started = self.listener.started_events[0]
            self.assertEqual(started.command.get("mechanism"), "SCRAM-SHA-256")

        # Step 3: verify auth failure conditions
        client = await self.async_rs_or_single_client_noauth(
            username="sha1", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-256"
        )
        with self.assertRaises(OperationFailure):
            await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="sha256", password="pwd", authSource="testscram", authMechanism="SCRAM-SHA-1"
        )
        with self.assertRaises(OperationFailure):
            await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="not-a-user", password="pwd", authSource="testscram"
        )
        with self.assertRaises(OperationFailure):
            await client.testscram.command("dbstats")

        if async_client_context.is_rs:
            host, port = await async_client_context.host, await async_client_context.port
            uri = "mongodb://both:pwd@%s:%d/testscram?replicaSet=%s" % (
                host,
                port,
                async_client_context.replica_set_name,
            )
            client = await self.async_single_client_noauth(uri)
            await client.testscram.command("dbstats")
            db = client.get_database("testscram", read_preference=ReadPreference.SECONDARY)
            await db.command("dbstats")

    @unittest.skipUnless(HAVE_STRINGPREP, "Cannot test without stringprep")
    async def test_scram_saslprep(self):
        # Step 4: test SASLprep
        host, port = await async_client_context.host, await async_client_context.port
        # Test the use of SASLprep on passwords. For example,
        # saslprep('\u2136') becomes 'IV' and saslprep('I\u00ADX')
        # becomes 'IX'. SASLprep is only supported when the standard
        # library provides stringprep.
        await async_client_context.create_user(
            "testscram", "\u2168", "\u2163", roles=["dbOwner"], mechanisms=["SCRAM-SHA-256"]
        )
        await async_client_context.create_user(
            "testscram", "IX", "IX", roles=["dbOwner"], mechanisms=["SCRAM-SHA-256"]
        )

        client = await self.async_rs_or_single_client_noauth(
            username="\u2168", password="\u2163", authSource="testscram"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="\u2168",
            password="\u2163",
            authSource="testscram",
            authMechanism="SCRAM-SHA-256",
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="\u2168", password="IV", authSource="testscram"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="IX", password="I\u00ADX", authSource="testscram"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="IX",
            password="I\u00ADX",
            authSource="testscram",
            authMechanism="SCRAM-SHA-256",
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            username="IX", password="IX", authSource="testscram", authMechanism="SCRAM-SHA-256"
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            "mongodb://\u2168:\u2163@%s:%d/testscram" % (host, port)
        )
        await client.testscram.command("dbstats")
        client = await self.async_rs_or_single_client_noauth(
            "mongodb://\u2168:IV@%s:%d/testscram" % (host, port)
        )
        await client.testscram.command("dbstats")

        client = await self.async_rs_or_single_client_noauth(
            "mongodb://IX:I\u00ADX@%s:%d/testscram" % (host, port)
        )
        await client.testscram.command("dbstats")
        client = await self.async_rs_or_single_client_noauth(
            "mongodb://IX:IX@%s:%d/testscram" % (host, port)
        )
        await client.testscram.command("dbstats")

    async def test_cache(self):
        client = await self.async_single_client()
        credentials = client.options.pool_options._credentials
        cache = credentials.cache
        self.assertIsNotNone(cache)
        self.assertIsNone(cache.data)
        # Force authentication.
        await client.admin.command("ping")
        cache = credentials.cache
        self.assertIsNotNone(cache)
        data = cache.data
        self.assertIsNotNone(data)
        self.assertEqual(len(data), 4)
        ckey, skey, salt, iterations = data
        self.assertIsInstance(ckey, bytes)
        self.assertIsInstance(skey, bytes)
        self.assertIsInstance(salt, bytes)
        self.assertIsInstance(iterations, int)

    @async_client_context.require_sync
    async def test_scram_threaded(self):
        coll = async_client_context.client.db.test
        await coll.drop()
        await coll.insert_one({"_id": 1})

        # The first thread to call find() will authenticate
        client = await self.async_rs_or_single_client()
        coll = client.db.test
        threads = []
        for _ in range(4):
            threads.append(AutoAuthenticateThread(coll))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            self.assertTrue(thread.success)


class TestAuthURIOptions(AsyncIntegrationTest):
    @async_client_context.require_auth
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await async_client_context.create_user("admin", "admin", "pass")
        await async_client_context.create_user(
            "pymongo_test", "user", "pass", ["userAdmin", "readWrite"]
        )

    async def asyncTearDown(self):
        await async_client_context.drop_user("pymongo_test", "user")
        await async_client_context.drop_user("admin", "admin")
        await super().asyncTearDown()

    async def test_uri_options(self):
        # Test default to admin
        host, port = await async_client_context.host, await async_client_context.port
        client = await self.async_rs_or_single_client_noauth(
            "mongodb://admin:pass@%s:%d" % (host, port)
        )
        self.assertTrue(await client.admin.command("dbstats"))

        if async_client_context.is_rs:
            uri = "mongodb://admin:pass@%s:%d/?replicaSet=%s" % (
                host,
                port,
                async_client_context.replica_set_name,
            )
            client = await self.async_single_client_noauth(uri)
            self.assertTrue(await client.admin.command("dbstats"))
            db = client.get_database("admin", read_preference=ReadPreference.SECONDARY)
            self.assertTrue(await db.command("dbstats"))

        # Test explicit database
        uri = "mongodb://user:pass@%s:%d/pymongo_test" % (host, port)
        client = await self.async_rs_or_single_client_noauth(uri)
        with self.assertRaises(OperationFailure):
            await client.admin.command("dbstats")
        self.assertTrue(await client.pymongo_test.command("dbstats"))

        if async_client_context.is_rs:
            uri = "mongodb://user:pass@%s:%d/pymongo_test?replicaSet=%s" % (
                host,
                port,
                async_client_context.replica_set_name,
            )
            client = await self.async_single_client_noauth(uri)
            with self.assertRaises(OperationFailure):
                await client.admin.command("dbstats")
            self.assertTrue(await client.pymongo_test.command("dbstats"))
            db = client.get_database("pymongo_test", read_preference=ReadPreference.SECONDARY)
            self.assertTrue(await db.command("dbstats"))

        # Test authSource
        uri = "mongodb://user:pass@%s:%d/pymongo_test2?authSource=pymongo_test" % (host, port)
        client = await self.async_rs_or_single_client_noauth(uri)
        with self.assertRaises(OperationFailure):
            await client.pymongo_test2.command("dbstats")
        self.assertTrue(await client.pymongo_test.command("dbstats"))

        if async_client_context.is_rs:
            uri = (
                "mongodb://user:pass@%s:%d/pymongo_test2?replicaSet="
                "%s;authSource=pymongo_test" % (host, port, async_client_context.replica_set_name)
            )
            client = await self.async_single_client_noauth(uri)
            with self.assertRaises(OperationFailure):
                await client.pymongo_test2.command("dbstats")
            self.assertTrue(await client.pymongo_test.command("dbstats"))
            db = client.get_database("pymongo_test", read_preference=ReadPreference.SECONDARY)
            self.assertTrue(await db.command("dbstats"))


if __name__ == "__main__":
    unittest.main()
