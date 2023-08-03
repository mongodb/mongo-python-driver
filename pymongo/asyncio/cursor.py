from greenletio import async_


class AsyncCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def __aiter__(self):
        return self

    def __next(self):
        try:
            return self.cursor.__next__()
        except StopIteration:
            raise StopAsyncIteration

    __anext__ = async_(__next)
