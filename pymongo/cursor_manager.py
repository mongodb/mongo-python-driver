"""Different managers to handle when cursors are killed after they are closed.

New cursor managers should be defined as subclasses of CursorManager and can be
installed on a connection by calling `pymongo.connection.Connection.set_cursor_manager`."""

import types

class CursorManager(object):
    """The default cursor manager.

    This manager will kill cursors one at a time as they are closed.
    """
    def __init__(self, connection):
        """Instantiate the manager.

        Arguments:
        - `connection`: a Mongo Connection
        """
        self.__connection = connection

    def close(self, cursor_id):
        """Close a cursor by killing it immediately.

        Raises TypeError if cursor_id is not an instance of (int, long).

        Arguments:
        - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (types.IntType, types.LongType)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__connection.kill_cursors([cursor_id])

class BatchCursorManager(CursorManager):
    """A cursor manager that kills cursors in batches.
    """
    def __init__(self, connection):
        """Instantiate the manager.

        Arguments:
        - `connection`: a Mongo Connection
        """
        self.__dying_cursors = []
        self.__max_dying_cursors = 20
        self.__connection = connection

        CursorManager.__init__(self, connection)

    def __del__(self):
        """Cleanup - be sure to kill any outstanding cursors.
        """
        self.__connection.kill_cursors(self.__dying_cursors)

    def close(self, cursor_id):
        """Close a cursor by killing it in a batch.

        Raises TypeError if cursor_id is not an instance of (int, long).

        Arguments:
        - `cursor_id`: cursor id to close
        """
        if not isinstance(cursor_id, (types.IntType, types.LongType)):
            raise TypeError("cursor_id must be an instance of (int, long)")

        self.__dying_cursors.append(cursor_id)

        if len(self.__dying_cursors) > self.__max_dying_cursors:
            self.__connection.kill_cursors(self.__dying_cursors)
            self.__dying_cursors = []
