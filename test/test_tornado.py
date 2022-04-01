import concurrent.futures
import json
import threading
import unittest

from pymongo import MongoClient

try:
    import tornado.ioloop
    from tornado.httpclient import HTTPClient
    from tornado.httpserver import HTTPServer
    from tornado.web import RequestHandler

    has_tornado = True
except ImportError:
    RequestHandler = object
    has_tornado = False


class MainHandler(RequestHandler):
    async def get(self):
        client = MongoClient()
        value = await client.test.command_async("ping")
        value = json.dumps(value, default=str)
        self.write(value)


def target():
    client = HTTPClient()
    response = client.fetch("http://localhost:8889")
    assert response.code == 200


def test_hello_world():
    if not has_tornado:
        unittest.skip("Requires tornado")
    futures = []
    app = tornado.web.Application(
        [
            (r"/", MainHandler),
        ]
    )
    app.listen(8889)
    server = HTTPServer(app)
    io_loop = tornado.ioloop.IOLoop.current()
    server_thread = threading.Thread(target=io_loop.start, daemon=True)
    server_thread.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(5):
            futures.append(executor.submit(target))
        concurrent.futures.wait(futures)
        for future in futures:
            future.result()
