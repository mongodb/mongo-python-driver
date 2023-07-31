class AsyncCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self.cursor.__next__()
        except StopIteration:
            raise StopAsyncIteration
