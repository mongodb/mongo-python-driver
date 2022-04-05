try:
    import gevent.monkey

    gevent.monkey.patch_all()

    import asyncio

    import asyncio_gevent

    has_gevent = True
except ImportError:
    has_gevent = False

import unittest


async def ping():
    from pymongo import MongoClient

    client = MongoClient()
    return await client.test.command_async("ping")


class TestAsyncioGevent(unittest.TestCase):
    def test_asyncio_gevent(self):
        asyncio.set_event_loop_policy(asyncio_gevent.EventLoopPolicy())
        future = ping()
        greenlet = asyncio_gevent.future_to_greenlet(future)
        greenlet.start()
        greenlet.join()
        value = greenlet.get()
        assert value["ok"]


if __name__ == "__main__":
    unittest.main()
