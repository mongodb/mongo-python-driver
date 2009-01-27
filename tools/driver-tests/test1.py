from pymongo.connection import Connection

db = Connection().test1
db.drop_collection("part1")
for i in range(100):
    db.part1.save({"x":  i})
