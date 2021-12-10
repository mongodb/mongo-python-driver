import json 
import glob
import os
from pymongo import MongoClient

HERE = os.path.abspath(os.path.dirname(__file__))

client = MongoClient()

for path in glob.glob(os.path.join(HERE, 'sample_data', '*.json')):
    with open(path) as fid:
        entries = [json.loads(line) for line in fid]
    for entry in entries:
        del entry['_id']

    name, _ = os.path.splitext(os.path.basename(path))
    print(name, len(entries))

    collection = client.sample_mflix[name]
    collection.insert_many(entries)