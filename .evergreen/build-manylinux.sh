#!/bin/bash -ex

docker version

docker pull quay.io/pypa/manylinux1_x86_64
docker pull quay.io/pypa/manylinux1_i686
docker pull quay.io/pypa/manylinux2014_x86_64
docker pull quay.io/pypa/manylinux2014_i686
docker pull quay.io/pypa/manylinux2014_aarch64
docker pull quay.io/pypa/manylinux2014_ppc64le
docker pull quay.io/pypa/manylinux2014_s390x

docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux1_x86_64 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux1_i686 linux32 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_x86_64 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_i686 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_aarch64 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_ppc64le /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_s390x /pymongo/.evergreen/build-wheels.sh

ls dist
