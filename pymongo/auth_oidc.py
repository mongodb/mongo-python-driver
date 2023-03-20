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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

import bson
from bson.binary import Binary
from bson.son import SON
from pymongo.errors import OperationFailure


@dataclass
class _OIDCProperties:
    on_oidc_request_token: Optional[Callable[..., Dict]]
    on_oidc_refresh_token: Optional[Callable[..., Dict]]
    provider_name: Optional[str]


"""Mechanism properties for MONGODB-OIDC authentication."""


@dataclass
class _OIDCCache:
    token_result: Optional[Dict]
    token_exp_utc: Optional[datetime]
    cache_exp_utc: datetime
    server_resp: Optional[Dict]
    lock: threading.Lock


class _OIDCMechanism:
    cache: Dict[str, _OIDCCache] = {}
    token_buffer_minutes = 5
    callback_timeout_seconds = 5 * 60
    cache_timeout_minutes = 60 * 5

    def get_cache_key(self, credentials, address):
        # Handle authorization code credentials.
        address = address
        principal_name = credentials.username
        properties = credentials.mechanism_properties
        request_cb = properties.on_oidc_request_token
        refresh_cb = properties.on_oidc_refresh_token
        return f"{principal_name}{address[0]}{address[1]}{id(request_cb)}{id(refresh_cb)}"

    def get_current_token(self, credentials, address, use_callbacks=True):
        properties: _OIDCProperties = credentials.mechanism_properties
        cache_key = self.get_cache_key(credentials, address)
        cache_value = self.cache[cache_key]
        principal_name = credentials.username

        request_cb = properties.on_oidc_request_token
        refresh_cb = properties.on_oidc_refresh_token
        if not use_callbacks:
            request_cb = None
            refresh_cb = None

        current_valid_token = False
        if cache_value.token_exp_utc is not None:
            now_utc = datetime.now(timezone.utc)
            exp_utc = cache_value.token_exp_utc
            buffer_seconds = self.token_buffer_minutes * 60
            if (exp_utc - now_utc).total_seconds() >= buffer_seconds:
                current_valid_token = True

        timeout = self.callback_timeout_seconds

        if not use_callbacks and not current_valid_token:
            return None

        if not current_valid_token and request_cb is not None:
            with cache_value.lock:
                if cache_value.token_result is None or refresh_cb is None:
                    cache_value.token_result = request_cb(
                        principal_name, cache_value.server_resp, timeout
                    )
                elif request_cb is not None:
                    cache_value.token_result = refresh_cb(
                        principal_name, cache_value.server_resp, cache_value.token_result, timeout
                    )
                cache_exp_utc = datetime.now(timezone.utc) + timedelta(
                    minutes=self.cache_timeout_minutes
                )
                cache_value.cache_exp_utc = cache_exp_utc

        token_result = cache_value.token_result
        if not isinstance(token_result, dict):
            raise ValueError("OIDC callback returned invalid result")
        if "access_token" not in token_result:
            raise ValueError("OIDC callback did not return an access_token")

        for key in token_result:
            if key not in ["access_token", "expires_in_seconds", "refresh_token"]:
                raise ValueError(f"OIDC callback returned invalid result key '{key}'")

        token = token_result["access_token"]
        if "expires_in_seconds" in token_result:
            expires_in = int(token_result["expires_in_seconds"])
            buffer_seconds = self.token_buffer_minutes * 60
            if expires_in >= buffer_seconds:
                now_utc = datetime.now(timezone.utc)
                exp_utc = now_utc + timedelta(seconds=expires_in)
                cache_value.token_exp_utc = exp_utc

        return token

    def invalidate_token(self, credentials, address):
        cache_key = self.get_cache_key(credentials, address)
        cache_value = self.cache.get(cache_key)
        if cache_value:
            cache_value.token_exp_utc = None

    def auth_start(self, credentials, address, use_callbacks=True):
        properties: _OIDCProperties = credentials.mechanism_properties

        # Clear out old items in the cache.
        now_utc = datetime.now(timezone.utc)
        to_remove = []
        for key, value in self.cache.items():
            if value.cache_exp_utc < now_utc:
                to_remove.append(key)
        for key in to_remove:
            del self.cache[key]

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

        cache_key = self.get_cache_key(credentials, address)
        cache_value = self.cache.get(cache_key)
        principal_name = credentials.username

        if cache_value is not None:
            cache_value.cache_exp_utc = datetime.now(timezone.utc) + timedelta(
                minutes=self.cache_timeout_minutes
            )

        if cache_value is None:
            lock = threading.Lock()
            cache_exp_utc = datetime.now(timezone.utc) + timedelta(
                minutes=self.cache_timeout_minutes
            )
            cache_value = _OIDCCache(
                lock=lock,
                token_result=None,
                server_resp=None,
                token_exp_utc=None,
                cache_exp_utc=cache_exp_utc,
            )
            self.cache[cache_key] = cache_value

        if cache_value.server_resp is None:
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

        token = self.get_current_token(credentials, address, use_callbacks)
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

    def authenticate(self, credentials, sock_info, reauthenticate):
        if reauthenticate:
            self.invalidate_token(credentials, sock_info.address)
        cache = self.cache
        ctx = sock_info.auth_ctx
        cmd = None
        cache_key = self.get_cache_key(credentials, sock_info.address)
        in_cache = cache_key in cache

        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
        else:
            cmd = self.auth_start(credentials, sock_info.address)
            try:
                resp = sock_info.command(credentials.source, cmd)
            except Exception:
                cache.pop(cache_key, None)
                # Allow for one retry on reauthenticate when callbacks are in use
                # and there was no cache.
                if (
                    reauthenticate
                    and not credentials.mechanism_properties.provider_name
                    and in_cache
                ):
                    return _authenticate_oidc(credentials, sock_info, False)
                raise

        if resp["done"]:
            return

        # Convert the server response to be more pythonic.
        # Avoid circular import
        from pymongo.common import camel_to_snake

        orig_server_resp: Dict = bson.decode(resp["payload"])
        server_resp = dict()
        for key, value in orig_server_resp.items():
            server_resp[camel_to_snake(key)] = value

        if "token_endpoint" in server_resp:
            cache[cache_key].server_resp = server_resp

        conversation_id = resp["conversationId"]
        token = self.get_current_token(credentials, sock_info.address)
        bin_payload = Binary(bson.encode(dict(jwt=token)))
        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", conversation_id),
                ("payload", bin_payload),
            ]
        )
        response = sock_info.command("$external", cmd)
        if not response["done"]:
            cache.pop(cache_key, None)
            raise OperationFailure("SASL conversation failed to complete.")


_internal = _OIDCMechanism()


class _OIDCContextMixin:
    credentials: Any
    address: Any

    def speculate_command(self):
        cmd = _internal.auth_start(self.credentials, self.address, False)
        if cmd is None:
            return
        cmd["db"] = self.credentials.source
        return cmd


def _authenticate_oidc(credentials, sock_info, reauthenticate):
    """Authenticate using MONGODB-OIDC."""
    return _internal.authenticate(credentials, sock_info, reauthenticate)
