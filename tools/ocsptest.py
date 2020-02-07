# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

import argparse
import logging
import socket

from ssl import CERT_REQUIRED

from pymongo.pyopenssl_context import SSLContext, PROTOCOL_SSLv23

logging.basicConfig(level=logging.DEBUG)

def check_ocsp(host, port, capath):
    ctx = SSLContext(PROTOCOL_SSLv23)
    ctx.verify_mode = CERT_REQUIRED
    if capath is not None:
        ctx.load_verify_locations(capath)
    else:
        ctx.set_default_verify_paths()

    s = socket.socket()
    s.connect((host, port))
    try:
        s = ctx.wrap_socket(s, server_hostname=host)
    finally:
        s.close()

def main():
    parser = argparse.ArgumentParser(
        description='Debug OCSP')
    parser.add_argument(
        '--host', type=str, required=True, help="Host to connect to")
    parser.add_argument(
        '-p', '--port', type=int, default=443, help="Port to connect to")
    parser.add_argument(
        '--ca_file', type=str, default=None, help="CA file for host")
    args = parser.parse_args()
    check_ocsp(args.host, args.port, args.ca_file)

if __name__ == '__main__':
     main()

