# Copyright 2009 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

sys.path[0:0] = os.path.join(os.getcwd(), "..")
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
