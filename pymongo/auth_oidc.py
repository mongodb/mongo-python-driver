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
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

import bson
from bson.binary import Binary
from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.helpers import _AUTHENTICATION_FAILED_CODE, _REAUTHENTICATION_REQUIRED_CODE


@dataclass
class _OIDCProperties:
    request_token_callback: Optional[Callable[..., Dict]]
    provider_name: Optional[str]
    allowed_hosts: List[str]


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
CALLBACK_TIMEOUT_SECONDS = 5 * 60
CACHE_TIMEOUT_MINUTES = 60 * 5
CALLBACK_VERSION = 0

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

    # Handle aws provider credentials.
    if properties.provider_name == "aws":
        properties.request_token_callback = _aws_callback

    request_cb = properties.request_token_callback

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

    # Get or create the cache item.
    cache_key = f"{principal_name}{address[0]}{address[1]}{id(request_cb)}"
    _CACHE.setdefault(cache_key, _OIDCAuthenticator(username=principal_name, properties=properties))

    return _CACHE[cache_key]


def _get_cache_exp():
    return datetime.now(timezone.utc) + timedelta(minutes=CACHE_TIMEOUT_MINUTES)


def _aws_callback(_, __):
    aws_identity_file = os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
    with open(aws_identity_file) as fid:
        token = fid.read().strip()
    return dict(access_token=token)


@dataclass
class _OIDCAuthenticator:
    username: str
    properties: _OIDCProperties
    idp_info: Optional[Dict] = field(default=None)
    idp_resp: Optional[Dict] = field(default=None)
    access_token: str = field(default="")
    cache_exp_utc: datetime = field(default_factory=_get_cache_exp)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def get_current_token(self, use_callbacks=True):
        properties = self.properties

        request_cb = properties.request_token_callback
        if not use_callbacks:
            request_cb = None

        current_token = self.access_token
        timeout = CALLBACK_TIMEOUT_SECONDS

        if not use_callbacks and not current_token:
            return None

        if not current_token and request_cb is not None:
            context = {
                "timeout_seconds": timeout,
                "version": CALLBACK_VERSION,
                "idp_resp": self.idp_resp,
            }

            self.idp_resp = request_cb(self.idp_info, context)
            cache_exp_utc = datetime.now(timezone.utc) + timedelta(minutes=CACHE_TIMEOUT_MINUTES)
            self.cache_exp_utc = cache_exp_utc
            current_token = self.idp_resp and self.idp_resp.get("access_token")

        if not current_token:
            raise ValueError("No current token")

        token_result = self.idp_resp

        # Validate callback return value.
        if not isinstance(token_result, dict):
            raise ValueError("OIDC callback returned invalid result")

        if "access_token" not in token_result:
            raise ValueError("OIDC callback did not return an access_token")

        expected = ["access_token", "expires_in_seconds", "refresh_token"]
        for key in token_result:
            if key not in expected:
                raise ValueError(f'Unexpected field in callback result "{key}"')

        return current_token

    def auth_start_cmd(self, use_callbacks=True):
        principal_name = self.username

        if self.idp_info is not None:
            self.cache_exp_utc = datetime.now(timezone.utc) + timedelta(
                minutes=CACHE_TIMEOUT_MINUTES
            )

        if self.idp_info is None:
            self.cache_exp_utc = _get_cache_exp()

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

        token = self.get_current_token(use_callbacks)
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

    def clear(self):
        self.idp_info = None
        self.idp_resp = None
        self.access_token = ""

    def authenticate(self, sock_info, reauthenticate=False):
        with self.lock:
            resp = None
            self._last_cmd = None
            self.sock_info = sock_info
            if reauthenticate:
                prev_token = getattr(sock_info, "oidc_access_token", 0)
                # If we haven't changed tokens, mark the current one as expired.
                if prev_token == self.access_token:
                    self.access_token = ""

            try:
                resp = self._authenticate()
            except OperationFailure as exc:
                resp = self._handle_retry(exc)

            assert self.idp_resp is not None
            access_token = self.access_token = self.idp_resp["access_token"]
            sock_info.oidc_access_token = access_token
            return resp

    def _authenticate(self):
        sock_info = self.sock_info
        ctx = sock_info.auth_ctx
        cmd = None

        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
        else:
            cmd = self.auth_start_cmd()
            resp = self._run_command(sock_info, cmd)

        if resp["done"]:
            return resp

        server_resp: Dict = bson.decode(resp["payload"])
        if "issuer" in server_resp:
            self.idp_info = server_resp

        conversation_id = resp["conversationId"]
        token = self.get_current_token()
        bin_payload = Binary(bson.encode({"jwt": token}))
        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", conversation_id),
                ("payload", bin_payload),
            ]
        )
        resp = self._run_command(sock_info, cmd)
        if not resp["done"]:
            self.clear()
            raise OperationFailure("SASL conversation failed to complete.")
        return resp

    def _run_command(self, sock_info, cmd):
        self._last_cmd = cmd
        return sock_info.command("$external", cmd, no_reauth=True)

    def _handle_retry(self, exc):
        if not self._last_cmd:
            raise
        sock_info = self.sock_info
        # Only retry on an auth failure code if we've had at least one
        # accepted token.
        prev_token = getattr(sock_info, "oidc_access_token", None)
        if exc.code == _REAUTHENTICATION_REQUIRED_CODE or (
            prev_token and exc.code == _AUTHENTICATION_FAILED_CODE
        ):
            payload = bson.decode(self._last_cmd["payload"])
            if "jwt" not in payload:
                raise exc
            # Retry from a clean slate.
            self.clear()
            return self._authenticate()
        else:
            # We cannot retry.
            self.clear()
            raise exc


def _authenticate_oidc(credentials, sock_info, reauthenticate):
    """Authenticate using MONGODB-OIDC."""
    authenticator = _get_authenticator(credentials, sock_info.address)
    return authenticator.authenticate(sock_info, reauthenticate)
