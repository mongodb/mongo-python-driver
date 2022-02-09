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

from pymongo.pyopenssl_context import SSLContext
from pymongo.ssl_support import get_ssl_context

# Enable logs in this format:
# 2020-06-08 23:49:35,982 DEBUG ocsp_support Peer did not staple an OCSP response
FORMAT = "%(asctime)s %(levelname)s %(module)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)


def check_ocsp(host, port, capath):
    ctx = get_ssl_context(
        None,  # certfile
        None,  # passphrase
        capath,  # ca_certs
        None,  # crlfile
        False,  # allow_invalid_certificates
        False,  # allow_invalid_hostnames
        False,
    )  # disable_ocsp_endpoint_check

    # Ensure we're using pyOpenSSL.
    assert isinstance(ctx, SSLContext)

    s = socket.socket()
    s.connect((host, port))
    try:
        s = ctx.wrap_socket(s, server_hostname=host)
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="Debug OCSP")
    parser.add_argument("--host", type=str, required=True, help="Host to connect to")
    parser.add_argument("-p", "--port", type=int, default=443, help="Port to connect to")
    parser.add_argument("--ca_file", type=str, default=None, help="CA file for host")
    args = parser.parse_args()
    check_ocsp(args.host, args.port, args.ca_file)


if __name__ == "__main__":
    main()
