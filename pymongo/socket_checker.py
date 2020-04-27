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

"""Select / poll helper"""

import errno
import select

_HAVE_POLL = hasattr(select, "poll")
_SelectError = getattr(select, "error", OSError)


def _errno_from_exception(exc):
    if hasattr(exc, 'errno'):
        return exc.errno
    if exc.args:
        return exc.args[0]
    return None


class SocketChecker(object):

    def __init__(self):
        if _HAVE_POLL:
            self._poller = select.poll()
        else:
            self._poller = None

    def select(self, sock, read=False, write=False, timeout=0):
        """Select for reads or writes with a timeout in seconds."""
        while True:
            try:
                if self._poller:
                    mask = select.POLLERR | select.POLLHUP
                    if read:
                        mask = mask | select.POLLIN | select.POLLPRI
                    if write:
                        mask = mask | select.POLLOUT
                    self._poller.register(sock, mask)
                    try:
                        # poll() timeout is in milliseconds. select()
                        # timeout is in seconds.
                        res = self._poller.poll(timeout * 1000)
                    finally:
                        self._poller.unregister(sock)
                else:
                    rlist = [sock] if read else []
                    wlist = [sock] if write else []
                    res = select.select(rlist, wlist, [sock], timeout)
            except (_SelectError, IOError) as exc:
                if _errno_from_exception(exc) in (errno.EINTR, errno.EAGAIN):
                    continue
                raise
            return res

    def socket_closed(self, sock):
        """Return True if we know socket has been closed, False otherwise.
        """
        try:
            res = self.select(sock, read=True)
        except (RuntimeError, KeyError):
            # RuntimeError is raised during a concurrent poll. KeyError
            # is raised by unregister if the socket is not in the poller.
            # These errors should not be possible since we protect the
            # poller with a mutex.
            raise
        except ValueError:
            # ValueError is raised by register/unregister/select if the
            # socket file descriptor is negative or outside the range for
            # select (> 1023).
            return True
        except Exception:
            # Any other exceptions should be attributed to a closed
            # or invalid socket.
            return True
        return any(res)
