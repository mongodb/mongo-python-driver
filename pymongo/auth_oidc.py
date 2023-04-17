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

""""MONGODB-OIDC Authentication helpers."""
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import bson
from bson.binary import Binary
from bson.son import SON
from pymongo.errors import OperationFailure, PyMongoError
from pymongo.helpers import _REAUTHENTICATION_REQUIRED_CODE


@dataclass
class _OIDCProperties:
    request_token_callback: Optional[Callable[..., Dict]]
    refresh_token_callback: Optional[Callable[..., Dict]]
    provider_name: Optional[str]
    allowed_hosts: List[str]


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
CALLBACK_TIMEOUT_SECONDS = 5 * 60
CACHE_TIMEOUT_MINUTES = 60 * 5


_CACHE: Dict[str, "_OIDCAuthenticator"] = {}


def _get_authenticator(credentials, address):
    # Clear out old items in the cache.
    now_utc = datetime.now(timezone.utc)
    to_remove = []
    for key, value in _CACHE.items():
        if value.cache_exp_utc is not None and value.cache_exp_utc < now_utc:
            to_remove.append(key)
    for key in to_remove:
        del _CACHE[key]

    # Extract values.
    principal_name = credentials.username
    properties = credentials.mechanism_properties
    request_cb = properties.request_token_callback
    refresh_cb = properties.refresh_token_callback

    # Validate that the address is allowed.
    if not properties.provider_name:
        found = False
        allowed_hosts = properties.allowed_hosts
        for patt in allowed_hosts:
            if patt == address[0]:
                found = True
            elif patt.startswith("*") and address[0].endswith(patt[1:]):
                found = True
        if not found:
            raise PyMongoError(
                f"Refusing to connect to {address[0]}, which is not in authOIDCAllowedHosts: {allowed_hosts}"
            )

    # Get or create the cache item.
    cache_key = f"{principal_name}{address[0]}{address[1]}{id(request_cb)}{id(refresh_cb)}"
    _CACHE.setdefault(cache_key, _OIDCAuthenticator(username=principal_name, properties=properties))

    return _CACHE[cache_key]


def _get_cache_exp():
    return datetime.now(timezone.utc) + timedelta(minutes=CACHE_TIMEOUT_MINUTES)


@dataclass
class _OIDCAuthenticator:
    username: str
    properties: _OIDCProperties
    idp_info: Optional[Dict] = field(default=None)
    idp_resp: Optional[Dict] = field(default=None)
    reauth_time: Optional[datetime] = field(default=None)
    idp_info_time: Optional[datetime] = field(default=None)
    token_exp_utc: Optional[datetime] = field(default=None)
    cache_exp_utc: datetime = field(default_factory=_get_cache_exp)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def get_current_token(self, use_callbacks=True):
        properties = self.properties
        principal_name = self.username

        request_cb = properties.request_token_callback
        refresh_cb = properties.refresh_token_callback
        if not use_callbacks:
            request_cb = None
            refresh_cb = None

        current_valid_token = False
        if self.token_exp_utc is not None:
            now_utc = datetime.now(timezone.utc)
            exp_utc = self.token_exp_utc
            buffer_seconds = TOKEN_BUFFER_MINUTES * 60
            if (exp_utc - now_utc).total_seconds() >= buffer_seconds:
                current_valid_token = True

        timeout = CALLBACK_TIMEOUT_SECONDS

        if not use_callbacks and not current_valid_token:
            return None

        if not current_valid_token and request_cb is not None:
            if self.idp_resp is None or refresh_cb is None:
                self.idp_resp = request_cb(principal_name, self.idp_info, timeout)
            elif request_cb is not None:
                self.idp_resp = refresh_cb(principal_name, self.idp_info, self.idp_resp, timeout)
            cache_exp_utc = datetime.now(timezone.utc) + timedelta(minutes=CACHE_TIMEOUT_MINUTES)
            self.cache_exp_utc = cache_exp_utc

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

        if "expires_in_seconds" in token_result:
            expires_in = int(token_result["expires_in_seconds"])
            buffer_seconds = TOKEN_BUFFER_MINUTES * 60
            if expires_in >= buffer_seconds:
                now_utc = datetime.now(timezone.utc)
                exp_utc = now_utc + timedelta(seconds=expires_in)
                self.token_exp_utc = exp_utc

        return token

    def auth_start_cmd(self, use_callbacks=True):
        properties = self.properties

        # Handle aws provider credentials.
        if properties.provider_name == "aws":
            aws_identity_file = os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
            with open(aws_identity_file) as fid:
                token = fid.read().strip()
            payload = dict(jwt=token)
            cmd = SON(
                [
                    ("saslStart", 1),
                    ("mechanism", "MONGODB-OIDC"),
                    ("payload", Binary(bson.encode(payload))),
                ]
            )
            return cmd

        principal_name = self.username

        if self.idp_info is not None:
            self.cache_exp_utc = datetime.now(timezone.utc) + timedelta(
                minutes=CACHE_TIMEOUT_MINUTES
            )

        if self.idp_info is None:
            self.cache_exp_utc = _get_cache_exp()

        if self.idp_info is None:
            # Send the SASL start with the optional principal name.
            payload = dict()

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

        token = self.get_current_token(use_callbacks)
        if not token:
            return None
        bin_payload = Binary(bson.encode(dict(jwt=token)))
        return SON(
            [
                ("saslStart", 1),
                ("mechanism", "MONGODB-OIDC"),
                ("payload", bin_payload),
            ]
        )

    def clear(self):
        self.idp_info = None
        self.idp_resp = None
        self.token_exp_utc = None

    def run_command(self, sock_info, cmd):
        try:
            return sock_info.command("$external", cmd)
        except OperationFailure as exc:
            self.clear()
            if exc.code == _REAUTHENTICATION_REQUIRED_CODE:
                if "jwt" in bson.decode(cmd["payload"]):
                    if (
                        self.idp_info_time is not None
                        and self.reauth_time is not None
                        and self.idp_info_time > self.reauth_time
                    ):
                        raise
                    self.handle_reauth(self.reauth_time)
                    return self.authenticate(sock_info)
            raise

    def handle_reauth(self, prev_time):
        new_time = self.reauth_time
        if prev_time and new_time and new_time <= prev_time:
            self.token_exp_utc = None
            if not self.properties.refresh_token_callback:
                self.clear()
        self.reauth_time = datetime.now(timezone.utc)

    def authenticate(self, sock_info):
        ctx = sock_info.auth_ctx
        cmd = None

        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
        else:
            cmd = self.auth_start_cmd()
            resp = self.run_command(sock_info, cmd)

        if resp["done"]:
            return

        # Convert the server response to be more pythonic.
        # Avoid circular import
        from pymongo.common import camel_to_snake

        orig_server_resp: Dict = bson.decode(resp["payload"])
        server_resp = dict()
        for key, value in orig_server_resp.items():
            server_resp[camel_to_snake(key)] = value

        if "issuer" in server_resp:
            self.idp_info = server_resp
            self.idp_info_time = datetime.now(timezone.utc)

        conversation_id = resp["conversationId"]
        token = self.get_current_token()
        bin_payload = Binary(bson.encode(dict(jwt=token)))
        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", conversation_id),
                ("payload", bin_payload),
            ]
        )
        resp = self.run_command(sock_info, cmd)
        if not resp["done"]:
            self.clear()
            raise OperationFailure("SASL conversation failed to complete.")
        return resp


class _OIDCContextMixin:
    credentials: Any
    address: Any

    def speculate_command(self):
        authenticator = _get_authenticator(self.credentials, self.address)
        with authenticator.lock:
            cmd = authenticator.auth_start_cmd(False)
        if cmd is None:
            return
        cmd["db"] = self.credentials.source
        return cmd


def _authenticate_oidc(credentials, sock_info, reauthenticate):
    """Authenticate using MONGODB-OIDC."""
    authenticator = _get_authenticator(credentials, sock_info.address)
    # Prevent a race condition on reauthentication.  Store the current time
    # and compare to reauth time.
    prev_time = authenticator.reauth_time
    with authenticator.lock:
        if reauthenticate:
            authenticator.handle_reauth(prev_time)
        return authenticator.authenticate(sock_info)
