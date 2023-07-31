import asyncio

from greenletio import await_
from greenletio.green.threading import Thread

import pymongo.asyncio.database
from bson import SON
from pymongo import MongoClient
from pymongo.asyncio.collection import Collection
from pymongo.database import Database


def test_database():
    async def async_main():
        command = SON(
            [("insert", "test"), ("ordered", False), ("documents", [{"hello2": "world2"}])]
        )
        client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
        db = pymongo.asyncio.database.Database(client, "test")
        await db.async_command(command)

    def main():
        client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
        db = Database(client, "test")
        # db.test.insert_one({"hello": "world"})
        found = db.test.find({"hello2": "world2"})
        for val in found:
            print(val)

    sync_thread = Thread(target=main)
    async_thread = Thread(target=await_(async_main()))

    async_thread.start()
    sync_thread.start()
    async_thread.join()
    sync_thread.join()


async def test_collection():
    client = MongoClient("mongodb://127.0.0.1:27017/", directConnection=True)
    db = Database(client, "test")
    collection = Collection(db, name="test")
    found = await collection.async_find({"hello2": "world2"})
    async for val in found:
        print(val)


# test_database()
asyncio.run(test_collection())
