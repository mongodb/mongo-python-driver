import asyncio

from greenletio import await_, patch_blocking

with patch_blocking():
    import time
    from threading import Thread

from bson import SON
from pymongo import MongoClient
from pymongo.asyncio.collection import Collection as AsyncCollection
from pymongo.asyncio.database import Database as AsyncDatabase
from pymongo.database import Database as SyncDatabase


def test_database():
    async def printer():
        while 1:
            print("sleep:  ", int(time.time()))
            await asyncio.sleep(1)

    def async_main_entry():
        await_(async_main())

    async def async_main():
        print("starting async!")
        task = asyncio.create_task(printer())

        command = SON(
            [("insert", "test"), ("ordered", False), ("documents", [{"hello2": "world2"}])]
        )
        client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
        db = AsyncDatabase(client, "test")
        for i in range(10):
            print("insert: ", int(time.time()))
            await db.async_command(command)
            await asyncio.sleep(1)
        task.cancel()
        client.close()
        await test_collection()
        print("ended async")

    def main():
        print("starting sync!")
        client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
        db = SyncDatabase(client, "test")
        # db.test.insert_one({"hello": "world"})
        for i in range(10):
            found = db.test.find_one({"hello2": "world2"})
            print("find:   ", int(time.time()))
            time.sleep(1)
        client.close()
        print("ended sync")

    sync_thread = Thread(target=main)
    async_thread = Thread(target=async_main_entry)

    """
    Notes: if we use synchronous function calls, it still blocks as if they were both on the main thread if they use any primitives from the stdlib.
    We probably need to somehow opt-in to using asyncio rather than basing
    it on if greenletio is installed.
    We could offer the equivalent of eventlet.monkey_patch and require that
    for the use of pymongo.asyncio
    """
    client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
    client.drop_database("test")
    client.close()

    async_thread.start()
    sync_thread.start()

    for i in range(10):
        print("waiting:", int(time.time()))
        time.sleep(1)

    async_thread.join()
    sync_thread.join()


async def test_collection():
    print("hello from test collection!")
    client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
    db = SyncDatabase(client, "test")
    collection = AsyncCollection(db, name="test")
    found = await collection.async_find({"hello2": "world2"})
    async for val in found:
        print(val)


test_database()
