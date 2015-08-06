# Copyright 2009-2015 MongoDB, Inc.
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

"""DEPRECATED - Different managers to handle when cursors are killed after
they are closed.

New cursor managers should be defined as subclasses of CursorManager and can be
installed on a connection by calling
`pymongo.connection.Connection.set_cursor_manager`.

.. versionchanged:: 2.1+
   Deprecated.
"""

import weakref


class CursorManager(object):
    """The default cursor manager.

    This manager will kill cursors one at a time as they are closed.
    """

    def __init__(self, connection):
        """Instantiate the manager.

        :Parameters:
          - `connection`: a Mongo Connection
        """
        self.__connection = weakref.ref(connection)

    def close(self, cursor_id):
        """Close a cursor by killing it immediately.

        Raises TypeError if cursor_id is not an instance of (int, long).

        :Parameters:
          - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__connection().kill_cursors([cursor_id])


class BatchCursorManager(CursorManager):
    """A cursor manager that kills cursors in batches.
    """

    def __init__(self, connection):
        """Instantiate the manager.

        :Parameters:
          - `connection`: a Mongo Connection
        """
        self.__dying_cursors = []
        self.__max_dying_cursors = 20
        self.__connection = weakref.ref(connection)

        CursorManager.__init__(self, connection)

    def __del__(self):
        """Cleanup - be sure to kill any outstanding cursors.
        """
        self.__connection().kill_cursors(self.__dying_cursors)

    def close(self, cursor_id):
        """Close a cursor by killing it in a batch.

        Raises TypeError if cursor_id is not an instance of (int, long).

        :Parameters:
          - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (int, long)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__dying_cursors.append(cursor_id)

        if len(self.__dying_cursors) > self.__max_dying_cursors:
            self.__connection().kill_cursors(self.__dying_cursors)
            self.__dying_cursors = []
