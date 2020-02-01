# Copyright 2020-present MongoDB, Inc.
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

"""MONGODB-AWS Authentication helpers."""

import os

try:

    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    import requests

    _HAVE_MONGODB_AWS = True
except ImportError:
    _HAVE_MONGODB_AWS = False

import bson


from base64 import standard_b64encode
from collections import namedtuple

from bson.binary import Binary
from bson.son import SON
from pymongo.errors import ConfigurationError, OperationFailure


_AWS_REL_URI = 'http://169.254.170.2/'
_AWS_EC2_URI = 'http://169.254.169.254/'
_AWS_EC2_PATH = 'latest/meta-data/iam/security-credentials/'
_AWS_HTTP_TIMEOUT = 10


_AWSCredential = namedtuple('_AWSCredential',
                            ['username', 'password', 'token'])
"""MONGODB-AWS credentials."""


def _aws_temp_credentials():
    """Construct temporary MONGODB-AWS credentials."""
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if access_key and secret_key:
        return _AWSCredential(
            access_key, secret_key, os.environ.get('AWS_SESSION_TOKEN'))
    # If the environment variable
    # AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is set then drivers MUST
    # assume that it was set by an AWS ECS agent and use the URI
    # http://169.254.170.2/$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI to
    # obtain temporary credentials.
    relative_uri = os.environ.get('AWS_CONTAINER_CREDENTIALS_RELATIVE_URI')
    if relative_uri is not None:
        try:
            res = requests.get(_AWS_REL_URI+relative_uri,
                               timeout=_AWS_HTTP_TIMEOUT)
            res_json = res.json()
        except (ValueError, requests.exceptions.RequestException):
            raise OperationFailure(
                'temporary MONGODB-AWS credentials could not be obtained')
    else:
        # If the environment variable AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is
        # not set drivers MUST assume we are on an EC2 instance and use the
        # endpoint
        # http://169.254.169.254/latest/meta-data/iam/security-credentials
        # /<role-name>
        # whereas role-name can be obtained from querying the URI
        # http://169.254.169.254/latest/meta-data/iam/security-credentials/.
        try:
            # Get token
            headers = {'X-aws-ec2-metadata-token-ttl-seconds': "30"}
            res = requests.post(_AWS_EC2_URI+'latest/api/token',
                                headers=headers, timeout=_AWS_HTTP_TIMEOUT)
            token = res.content
            # Get role name
            headers = {'X-aws-ec2-metadata-token': token}
            res = requests.get(_AWS_EC2_URI+_AWS_EC2_PATH, headers=headers,
                               timeout=_AWS_HTTP_TIMEOUT)
            role = res.text
            # Get temp creds
            res = requests.get(_AWS_EC2_URI+_AWS_EC2_PATH+role,
                               headers=headers, timeout=_AWS_HTTP_TIMEOUT)
            res_json = res.json()
        except (ValueError, requests.exceptions.RequestException):
            raise OperationFailure(
                'temporary MONGODB-AWS credentials could not be obtained')

    try:
        temp_user = res_json['AccessKeyId']
        temp_password = res_json['SecretAccessKey']
        token = res_json['Token']
    except KeyError:
        # If temporary credentials cannot be obtained then drivers MUST
        # fail authentication and raise an error.
        raise OperationFailure(
            'temporary MONGODB-AWS credentials could not be obtained')

    return _AWSCredential(temp_user, temp_password, token)


_AWS4_HMAC_SHA256 = 'AWS4-HMAC-SHA256'
_AWS_SERVICE = 'sts'


def _get_region(sts_host):
    """"""
    parts = sts_host.split('.')
    if len(parts) == 1 or sts_host == 'sts.amazonaws.com':
        return 'us-east-1'  # Default

    if len(parts) > 2 or not all(parts):
        raise OperationFailure("Server returned an invalid sts host")

    return parts[1]


def _aws_auth_header(credentials, server_nonce, sts_host):
    """Signature Version 4 Signing Process to construct the authorization header
    """
    region = _get_region(sts_host)

    request_parameters = 'Action=GetCallerIdentity&Version=2011-06-15'
    encoded_nonce = standard_b64encode(server_nonce).decode('utf8')
    request_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': str(len(request_parameters)),
        'Host': sts_host,
        'X-MongoDB-Server-Nonce': encoded_nonce,
        'X-MongoDB-GS2-CB-Flag': 'n',
    }
    request = AWSRequest(method="POST", url="/", data=request_parameters,
                         headers=request_headers)
    boto_creds = Credentials(credentials.username, credentials.password,
                             token=credentials.token)
    auth = SigV4Auth(boto_creds, "sts", region)
    auth.add_auth(request)
    final = {
        'a': request.headers['Authorization'],
        'd': request.headers['X-Amz-Date']
    }
    if credentials.token:
        final['t'] = credentials.token
    return final


def _auth_aws(credentials, sock_info):
    """Authenticate using MONGODB-AWS.
    """
    if not _HAVE_MONGODB_AWS:
        raise ConfigurationError(
            "MONGODB-AWS authentication requires botocore and requests: "
            "install these libraries with: "
            "python -m pip install 'pymongo[aws]'")

    if sock_info.max_wire_version < 9:
        raise ConfigurationError(
            "MONGODB-AWS authentication requires MongoDB version 4.4 or later")

    # If a username and password are not provided, drivers MUST query
    # a link-local AWS address for temporary credentials.
    if credentials.username is None:
        credentials = _aws_temp_credentials()

    # Client first.
    client_nonce = os.urandom(32)
    payload = {'r': Binary(client_nonce), 'p': 110}
    client_first = SON([('saslStart', 1),
                        ('mechanism', 'MONGODB-AWS'),
                        ('payload', Binary(bson.encode(payload)))])
    server_first = sock_info.command('$external', client_first)

    server_payload = bson.decode(server_first['payload'])
    server_nonce = server_payload['s']
    if len(server_nonce) != 64 or not server_nonce.startswith(client_nonce):
        raise OperationFailure("Server returned an invalid nonce.")
    sts_host = server_payload['h']
    if len(sts_host) < 1 or len(sts_host) > 255 or '..' in sts_host:
        # Drivers must also validate that the host is greater than 0 and less
        # than or equal to 255 bytes per RFC 1035.
        raise OperationFailure("Server returned an invalid sts host.")

    payload = _aws_auth_header(credentials, server_nonce, sts_host)
    client_second = SON([('saslContinue', 1),
                         ('conversationId', server_first['conversationId']),
                         ('payload', Binary(bson.encode(payload)))])
    res = sock_info.command('$external', client_second)
    if not res['done']:
        raise OperationFailure('MONGODB-AWS conversation failed to complete.')
