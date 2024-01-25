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

import abc
import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Union

import bson
from bson.binary import Binary
from pymongo._csot import remaining
from pymongo.azure_helpers import _get_azure_response
from pymongo.errors import ConfigurationError, OperationFailure

if TYPE_CHECKING:
    from pymongo.auth import MongoCredential
    from pymongo.pool import Connection


@dataclass
class OIDCIdPInfo:
    issuer: str
    clientId: str
    requestScopes: Optional[list[str]] = field(default=None)


@dataclass
class OIDCCallbackContext:
    timeout_seconds: float
    version: int
    refresh_token: Optional[str] = field(default=None)
    idp_info: Optional[OIDCIdPInfo] = field(default=None)


@dataclass
class OIDCCallbackResult:
    access_token: str
    expires_in_seconds: Optional[float] = field(default=None)
    refresh_token: Optional[str] = field(default=None)


class OIDCCallback(abc.ABC):
    """A base class for defining OIDC callbacks."""

    @abc.abstractmethod
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        """Convert the given BSON value into our own type."""


@dataclass
class _OIDCProperties:
    callback: Optional[OIDCCallback] = field(default=None)
    human_callback: Optional[OIDCCallback] = field(default=None)
    provider_name: Optional[str] = field(default=None)
    allowed_hosts: list[str] = field(default_factory=list)


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
HUMAN_CALLBACK_TIMEOUT_SECONDS = 5 * 60
CALLBACK_VERSION = 1
MACHINE_CALLBACK_TIMEOUT_SECONDS = 60
TIME_BETWEEN_CALLS_SECONDS = 0.1


def _get_authenticator(
    credentials: MongoCredential, address: tuple[str, int]
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


class _OIDCAWSCallback(OIDCCallback):
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
        if not token_file:
            raise RuntimeError(
                'MONGODB-OIDC with an "aws" provider requires "AWS_WEB_IDENTITY_TOKEN_FILE" to be set'
            )
        with open(token_file) as fid:
            return OIDCCallbackResult(access_token=fid.read().strip())


class _OIDCAzureCallback(OIDCCallback):
    def __init__(self, token_audience: str, username: Optional[str]) -> None:
        self.token_audience = token_audience
        self.username = username

    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        resp = _get_azure_response(self.token_audience, self.username, context.timeout_seconds)
        return OIDCCallbackResult(
            access_token=resp["access_token"], expires_in_seconds=resp["expires_in"]
        )


@dataclass
class _OIDCAuthenticator:
    username: str
    properties: _OIDCProperties
    refresh_token: Optional[str] = field(default=None)
    access_token: Optional[str] = field(default=None)
    idp_info: Optional[OIDCIdPInfo] = field(default=None)
    token_gen_id: int = field(default=0)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_call_time: float = field(default=0)

    def reauthenticate(self, conn: Connection) -> Optional[Mapping[str, Any]]:
        """Handle a reauthenticate from the server."""
        # Invalidate the token for the connection.
        self._invalidate(conn)
        # Call the appropriate auth logic for the callback type.
        if self.properties.callback:
            return self._authenticate_machine(conn)
        return self._authenticate_human(conn)

    def authenticate(self, conn: Connection) -> None:
        """Handle an initial authenticate request."""
        # First handle speculative auth.
        # It it succeeded, we are done.
        ctx = conn.auth_ctx
        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
            if resp["done"]:
                conn.oidc_token_gen_id = self.token_gen_id
                return None
            # If it is not done and we are a human callback, continue the conversation.
            elif self.properties.human_callback:
                return self._sasl_continue_jwt(conn, resp)

        # If spec auth failed, call the appropriate auth logic for the callback type.
        # We cannot assume that the token is invalid, because a proxy may have been
        # involved that stripped the speculative auth information.
        if self.properties.human_callback:
            return self._authenticate_human(conn)
        return self._authenticate_machine(conn)

    def get_spec_auth_cmd(self) -> Optional[Mapping[str, Any]]:
        """Get the appropriate speculative auth command."""
        access_token = self._get_access_token(False)
        if access_token:
            payload = {"jwt": access_token}
            return self._get_start_command(payload)
        if self.properties.callback:
            return None
        if self.idp_info is not None:
            return None
        return self._get_start_command(None)

    def _authenticate_machine(self, conn: Connection) -> Mapping[str, Any]:
        # If there is a cached access token, try to authenticate with it. If
        # authentication fails, it's possible the cached access token is expired. In
        # that case, invalidate the access token, fetch a new access token, and try
        # to authenticate again.
        if self.access_token:
            try:
                return self._sasl_start_jwt(conn)
            except Exception:  # noqa: S110
                pass
        return self._sasl_start_jwt(conn)

    def _authenticate_human(self, conn: Connection) -> Optional[Mapping[str, Any]]:
        # If we have a cached access token, try a JwtStepRequest.
        if self.access_token:
            try:
                return self._sasl_start_jwt(conn)
            except Exception:  # noqa: S110
                pass

        # If we have a cached refresh token, try a JwtStepRequest with that.
        if self.refresh_token:
            try:
                return self._sasl_start_jwt(conn)
            except Exception:  # noqa: S110
                pass

        # Start a new Two-Step SASL conversation.
        # Run a PrincipalStepRequest to get the IdpInfo.
        cmd = self._get_start_command(None)
        start_resp = self._run_command(conn, cmd)
        # Attempt to authenticate with a JwtStepRequest.
        return self._sasl_continue_jwt(conn, start_resp)

    def _get_access_token(self, use_human_callback: bool = True) -> Optional[str]:
        properties = self.properties
        cb: Union[None, OIDCCallback]
        resp: OIDCCallbackResult

        is_human = properties.human_callback is not None
        if is_human and self.idp_info is None:
            return None

        if properties.callback:
            cb = properties.callback
        if not use_human_callback and is_human:
            cb = None

        prev_token = self.access_token
        if prev_token:
            return prev_token

        if cb is None and not prev_token:
            return None

        if not prev_token and cb is not None:
            with self.lock:
                # See if the token was changed while we were waiting for the
                # lock.
                new_token = self.access_token
                if new_token != prev_token:
                    return new_token

                # Ensure that we are waiting a min time between callback invocations.
                delta = time.time() - self.last_call_time
                if delta < TIME_BETWEEN_CALLS_SECONDS:
                    time.sleep(TIME_BETWEEN_CALLS_SECONDS - delta)
                self.last_call_time = time.time()

                if is_human:
                    timeout = HUMAN_CALLBACK_TIMEOUT_SECONDS
                    assert self.idp_info is not None
                else:
                    timeout = int(remaining() or MACHINE_CALLBACK_TIMEOUT_SECONDS)

                context = OIDCCallbackContext(
                    timeout_seconds=timeout,
                    version=CALLBACK_VERSION,
                    refresh_token=self.refresh_token,
                    idp_info=self.idp_info,
                )
                resp = cb.fetch(context)
                if not isinstance(resp, OIDCCallbackResult):
                    raise ValueError("Callback result must be of type OIDCCallbackResult")
                self.refresh_token = resp.refresh_token
                self.access_token = resp.access_token
                self.token_gen_id += 1

        return self.access_token

    def _run_command(
        self, conn: Connection, cmd: MutableMapping[str, Any]
    ) -> Optional[Mapping[str, Any]]:
        try:
            return conn.command("$external", cmd, no_reauth=True)  # type: ignore[call-arg]
        except OperationFailure:
            self._invalidate(conn)
            raise

    def _invalidate(self, conn: Connection) -> None:
        # Ignore the invalidation if a token gen id is given and is less than our
        # current token gen id.
        token_gen_id = conn.oidc_token_gen_id or 0
        if token_gen_id is not None and token_gen_id < self.token_gen_id:
            return
        self.access_token = None

    def _sasl_continue_jwt(
        self, conn: Connection, start_resp: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        self.access_token = None
        self.refresh_token = None
        start_payload: dict = bson.decode(start_resp["payload"])
        if "issuer" in start_payload:
            self.idp_info = OIDCIdPInfo(**start_payload)
        access_token = self._get_access_token()
        conn.oidc_token_gen_id = self.token_gen_id
        cmd = self._get_continue_command({"jwt": access_token}, start_resp)
        return self._run_command(conn, cmd)

    def _sasl_start_jwt(self, conn: Connection) -> Mapping[str, Any]:
        access_token = self._get_access_token()
        conn.oidc_token_gen_id = self.token_gen_id
        cmd = self._get_start_command({"jwt": access_token})
        return self._run_command(conn, cmd)

    def _get_start_command(self, payload) -> Mapping[str, Any]:
        if payload is None:
            principal_name = self.username
            if principal_name:
                payload = {"n": principal_name}
            else:
                payload = {}
        bin_payload = Binary(bson.encode(payload))
        return {"saslStart": 1, "mechanism": "MONGODB-OIDC", "payload": bin_payload}

    def _get_continue_command(self, payload, start_resp) -> Mapping[str, Any]:
        bin_payload = Binary(bson.encode(payload))
        return {
            "saslContinue": 1,
            "payload": bin_payload,
            "conversationId": start_resp["conversationId"],
        }


def _authenticate_oidc(
    credentials: MongoCredential, conn: Connection, reauthenticate: bool
) -> Optional[Mapping[str, Any]]:
    """Authenticate using MONGODB-OIDC."""
    authenticator = _get_authenticator(credentials, conn.address)
    if reauthenticate:
        return authenticator.reauthenticate(conn)
    else:
        return authenticator.authenticate(conn)
