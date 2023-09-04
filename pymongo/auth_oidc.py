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

"""MONGODB-OIDC Authentication helpers."""
from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
)

import bson
from bson.binary import Binary
from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.helpers import _REAUTHENTICATION_REQUIRED_CODE

if TYPE_CHECKING:
    from pymongo.auth import MongoCredential
    from pymongo.pool import Connection


@dataclass
class _OIDCProperties:
    request_token_callback: Optional[Callable[..., Dict]]
    provider_name: Optional[str]
    allowed_hosts: List[str]


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
CALLBACK_TIMEOUT_SECONDS = 5 * 60
CALLBACK_VERSION = 0


def _get_authenticator(
    credentials: MongoCredential, address: Tuple[str, int]
) -> _OIDCAuthenticator:
    if credentials.cache.data:
        return credentials.cache.data

    # Extract values.
    principal_name = credentials.username
    properties = credentials.mechanism_properties

    # Validate that the address is allowed.
    if not properties.provider_name:
        found = False
        allowed_hosts = properties.allowed_hosts
        for patt in allowed_hosts:
            if patt == address[0]:
                found = True
            elif patt.startswith("*.") and address[0].endswith(patt[1:]):
                found = True
        if not found:
            raise ConfigurationError(
                f"Refusing to connect to {address[0]}, which is not in authOIDCAllowedHosts: {allowed_hosts}"
            )

    # Get or create the cache data.
    credentials.cache.data = _OIDCAuthenticator(username=principal_name, properties=properties)
    return credentials.cache.data


@dataclass
class _OIDCAuthenticator:
    username: str
    properties: _OIDCProperties
    idp_info: Optional[Dict] = field(default=None)
    idp_resp: Optional[Dict] = field(default=None)
    reauth_gen_id: int = field(default=0)
    idp_info_gen_id: int = field(default=0)
    token_gen_id: int = field(default=0)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def get_current_token(self, use_callback: bool = True) -> Optional[str]:
        properties = self.properties

        request_cb = properties.request_token_callback
        if not use_callback:
            request_cb = None

        current_valid_token = False

        timeout = CALLBACK_TIMEOUT_SECONDS
        if not use_callback and not current_valid_token:
            return None

        if not current_valid_token and request_cb is not None:
            prev_token = self.idp_resp["access_token"] if self.idp_resp else None
            with self.lock:
                # See if the token was changed while we were waiting for the
                # lock.
                new_token = self.idp_resp["access_token"] if self.idp_resp else None
                if new_token != prev_token:
                    return new_token

                refresh_token = self.idp_resp and self.idp_resp.get("refresh_token")
                refresh_token = refresh_token or ""
                context = {
                    "timeout_seconds": timeout,
                    "version": CALLBACK_VERSION,
                    "refresh_token": refresh_token,
                }

                if self.idp_resp is None:
                    self.idp_resp = request_cb(self.idp_info, context)
                self.token_gen_id += 1

        token_result = self.idp_resp

        # Validate callback return value.
        if not isinstance(token_result, dict):
            raise ValueError("OIDC callback returned invalid result")

        if "access_token" not in token_result:
            raise ValueError("OIDC callback did not return an access_token")

        expected = ["access_token", "expires_in_seconds", "refesh_token"]
        for key in token_result:
            if key not in expected:
                raise ValueError(f'Unexpected field in callback result "{key}"')

        token = token_result["access_token"]
        return token

    def auth_start_cmd(self, use_callback: bool = True) -> Optional[SON[str, Any]]:
        properties = self.properties

        # Handle aws provider credentials.
        if properties.provider_name == "aws":
            aws_identity_file = os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
            with open(aws_identity_file) as fid:
                token: Optional[str] = fid.read().strip()
            payload = {"jwt": token}
            cmd = SON(
                [
                    ("saslStart", 1),
                    ("mechanism", "MONGODB-OIDC"),
                    ("payload", Binary(bson.encode(payload))),
                ]
            )
            return cmd

        principal_name = self.username

        if self.idp_info is None:
            # Send the SASL start with the optional principal name.
            payload = {}

            if principal_name:
                payload["n"] = principal_name

            cmd = SON(
                [
                    ("saslStart", 1),
                    ("mechanism", "MONGODB-OIDC"),
                    ("payload", Binary(bson.encode(payload))),
                    ("autoAuthorize", 1),
                ]
            )
            return cmd

        token = self.get_current_token(use_callback)
        if not token:
            return None
        bin_payload = Binary(bson.encode({"jwt": token}))
        return SON(
            [
                ("saslStart", 1),
                ("mechanism", "MONGODB-OIDC"),
                ("payload", bin_payload),
            ]
        )

    def clear(self) -> None:
        self.idp_info = None
        self.idp_resp = None

    def run_command(
        self, conn: Connection, cmd: MutableMapping[str, Any]
    ) -> Optional[Mapping[str, Any]]:
        try:
            return conn.command("$external", cmd, no_reauth=True)  # type: ignore[call-arg]
        except OperationFailure as exc:
            self.clear()
            if exc.code == _REAUTHENTICATION_REQUIRED_CODE:
                if "jwt" in bson.decode(cmd["payload"]):
                    if self.idp_info_gen_id > self.reauth_gen_id:
                        raise
                    return self.authenticate(conn, reauthenticate=True)
            raise

    def authenticate(
        self, conn: Connection, reauthenticate: bool = False
    ) -> Optional[Mapping[str, Any]]:
        if reauthenticate:
            prev_id = getattr(conn, "oidc_token_gen_id", None)
            # Check if we've already changed tokens.
            if prev_id == self.token_gen_id:
                self.reauth_gen_id = self.idp_info_gen_id
                self.clear()

        ctx = conn.auth_ctx
        cmd = None

        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
        else:
            cmd = self.auth_start_cmd()
            assert cmd is not None
            resp = self.run_command(conn, cmd)

        assert resp is not None
        if resp["done"]:
            conn.oidc_token_gen_id = self.token_gen_id
            return None

        server_resp: Dict = bson.decode(resp["payload"])
        if "issuer" in server_resp:
            self.idp_info = server_resp
            self.idp_info_gen_id += 1

        conversation_id = resp["conversationId"]
        token = self.get_current_token()
        conn.oidc_token_gen_id = self.token_gen_id
        bin_payload = Binary(bson.encode({"jwt": token}))
        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", conversation_id),
                ("payload", bin_payload),
            ]
        )
        resp = self.run_command(conn, cmd)
        assert resp is not None
        if not resp["done"]:
            self.clear()
            raise OperationFailure("SASL conversation failed to complete.")
        return resp


def _authenticate_oidc(
    credentials: MongoCredential, conn: Connection, reauthenticate: bool
) -> Optional[Mapping[str, Any]]:
    """Authenticate using MONGODB-OIDC."""
    authenticator = _get_authenticator(credentials, conn.address)
    return authenticator.authenticate(conn, reauthenticate=reauthenticate)
