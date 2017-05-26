
from pymongo import MongoClient

def test_failure():
    m = MongoClient('mongodb://localhost:27019/?replicaSet=rs0')
    m.db.test.find_one()

def test_success():
    m = MongoClient('mongodb://localhost:27019/?replicaSet=rs0&tags=internal_public')
    m.db.test.find_one()
    MongoClient('mongodb://localhost:27017,localhost:27019/?replicaSet=rs0', tags='internal-public')
    m.db.test.find_one()
    print "success!"
