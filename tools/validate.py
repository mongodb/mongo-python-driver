import sys

from pymongo.bson import BSON
from pymongo.son import SON

def main():
    xml_file = sys.argv[1]
    out_file = sys.argv[2]

    f = open(xml_file, "r")
    xml = f.read()
    f.close()

    f = open(out_file, "w")
    doc = SON.from_xml(xml)
    bson = BSON.from_dict(doc)
    f.write(bson)
    f.close()

    assert doc == bson.to_dict()

if __name__ == "__main__":
    main()
