# Copyright 2013 10gen, Inc.
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

"""Authentication helpers."""

try:
    import hashlib
    _MD5 = hashlib.md5
except ImportError:  # for Python < 2.5
    import md5
    _MD5 = md5.new

from bson.son import SON


def _password_digest(username, password):
    """Get a password digest to use for authentication.
    """
    if not isinstance(password, basestring):
        raise TypeError("password must be an instance "
                        "of %s" % (basestring.__name__,))
    if len(password) == 0:
        raise TypeError("password can't be empty")
    if not isinstance(username, basestring):
        raise TypeError("username must be an instance "
                        "of %s" % (basestring.__name__,))

    md5hash = _MD5()
    data = "%s:mongo:%s" % (username, password)
    md5hash.update(data.encode('utf-8'))
    return unicode(md5hash.hexdigest())


def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = _MD5()
    data = "%s%s%s" % (nonce, unicode(username), digest)
    md5hash.update(data.encode('utf-8'))
    return unicode(md5hash.hexdigest())


def authenticate(credentials, sock_info, cmd_func):
    """Authenticate sock_info using credentials.
    """
    source, username, password = credentials
    # Get a nonce
    response, _ = cmd_func(sock_info, source, {'getnonce': 1})
    nonce = response['nonce']
    key = _auth_key(nonce, username, password)

    # Actually authenticate
    query = SON([('authenticate', 1),
                 ('user', username),
                 ('nonce', nonce),
                 ('key', key)])
    cmd_func(sock_info, source, query)

