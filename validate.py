import sys

from bson import BSON
from son import SON

def main():
    xml_file = sys.argv[1]
    out_file = sys.argv[2]

    f = open(xml_file, "r")
    xml = f.read()
    f.close()

    f = open(out_file, "w")
    f.write(BSON.from_dict(SON.from_xml(xml)))
    f.close()

if __name__ == "__main__":
    main()
