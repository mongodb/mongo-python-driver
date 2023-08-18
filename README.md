# mongo-python-driver-proxy
PyMongo proxy - http/socks proxy support with python-socks.

About
=====
This repo forked from [Pymongo](http://github.com/mongodb/mongo-python-driver) official repo for http/socks proxy support with the help of [python-socks](https://github.com/romis2012/python-socks).
DNS resolving done by pymongo itself. Connection done by python-socks. For more information please refer to [Pymongo](http://github.com/mongodb/mongo-python-driver) and about proxies refer to [python-socks](https://github.com/romis2012/python-socks).

How to install
========
If you installed pymongo before you need to remove it first:

    pip uninstall pymongo

Install it with git:

    pip install git+https://github.com/benjamintenny/mongo-python-driver-proxy

You can also download the project source and then install it:

    pip install .


Examples
========
Here's a basic example about using pymongo with proxies:

```python
import os
import pymongo
# Set socks5 proxy
os.environ["MONGO_PROXY"] = "socks5://127.0.0.1:1080"
# Connect to db as normal
client = pymongo.MongoClient("your-mongodb-url")
db = client.test
print(db.name)
```

Also you can:

Socks5 with authentication:
```python
os.environ["MONGO_PROXY"] = "socks5://username:password@127.0.0.1:1080"
```

HTTP proxy with authentication:
```python
os.environ["MONGO_PROXY"] = "http://username:password@127.0.0.1:8080"
```

Socks4 proxy:
```python
os.environ["MONGO_PROXY"] = "socks4://127.0.0.1:1080"
```
