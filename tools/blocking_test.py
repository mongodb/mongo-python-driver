import asyncio
import time

import dns
from dns import asyncresolver, resolver
from greenletio import async_, await_

# from pymongo.srv_resolver import _resolve


async def printer():
    while 1:
        print("sleep:  ", int(time.time()))
        await asyncio.sleep(1)


async def blocking():
    while 1:
        val = await async_(resolver.resolve)(
            "_mongodb._tcp.test1.test.build.10gen.cc", "SRV", lifetime=20.0
        )
        print("Finished DNS: ", val)
        await asyncio.sleep(1)


async def main():
    nonblocking = asyncio.create_task(printer())
    should_not_block = asyncio.create_task(blocking())

    await asyncio.gather(nonblocking, should_not_block)


asyncio.run(main())
