# Copyright 2009 10gen, Inc.
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

"""Some utilities for dealing with threading."""

import threading
import time


class TimeoutableLock(object):
    """Lock implementation that allows blocking acquires to timeout.
    """

    def __init__(self):
        self.__unlocked = threading.Event()
        self.__unlocked.set()
        self.__lock = threading.Lock()

    def acquire(self, blocking=True, timeout=None):
        """Acquire the lock, blocking or non-blocking.

        When invoked without arguments, block until the lock is unlocked, then
        set it to locked, and return True.

        When invoked with the `blocking` argument set to True, do the same
        thing as when called without arguments, and return True.

        When invoked with the `blocking` argument set to False, do not block.
        If a call without an argument would block, return False immediately;
        otherwise do the same thing as when called without arguments, and
        return True.

        If `blocking` is True and `timeout` is not None then `timeout`
        specifies a timeout in seconds. If the lock cannot be acquired within
        the time limit return False. If `blocking` is False then `timeout` is
        ignored.

        :Parameters:
          - `blocking` (optional): perform a blocking acquire
          - `timeout` (optional): add a time limit to a blocking acquire
        """
        did_acquire = False

        self.__lock.acquire()

        if self.__unlocked.isSet():
            self.__unlocked.clear()
            did_acquire = True
        elif blocking:
            if timeout is not None:
                start_blocking = time.time()
            while True:
                self.__lock.release()

                if timeout is not None:
                    self.__unlocked.wait(start_blocking + timeout - \
                                             time.time())
                else:
                    self.__unlocked.wait()

                self.__lock.acquire()

                if self.__unlocked.isSet():
                    self.__unlocked.clear()
                    did_acquire = True
                    break
                elif timeout is not None and \
                        time.time() > start_blocking + timeout:
                    break

        self.__lock.release()
        return did_acquire

    def release(self):
        """Release the lock.

        When the lock is locked, reset it to unlocked and return. If any other
        threads are blocked waiting for the lock to become unlocked, allow
        exactly one of them to proceed.

        Do not call this method when the lock is unlocked - a RuntimeError will
        be raised.

        There is no return value.
        """
        self.__lock.acquire()
        if self.__unlocked.isSet():
            self.__lock.release()
            raise RuntimeError("trying to release an unlocked TimeoutableLock")

        self.__unlocked.set()
        self.__lock.release()
