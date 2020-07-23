#!/bin/bash -ex

docker version

docker pull quay.io/pypa/manylinux1_x86_64
docker pull quay.io/pypa/manylinux1_i686
docker pull quay.io/pypa/manylinux2014_x86_64
docker pull quay.io/pypa/manylinux2014_i686
# This works on macOS locally but not on linux in evergreen.
# [2020/07/23 00:24:00.482] + docker run --rm -v /data/mci/cd100cec6341abda533450fb3f2fab99/src:/pymongo quay.io/pypa/manylinux2014_aarch64 /pymongo/.evergreen/build-wheels.sh
# [2020/07/23 00:24:01.186] standard_init_linux.go:211: exec user process caused "exec format error"
#
# Could be related to:
# https://github.com/pypa/manylinux/issues/410
#docker pull quay.io/pypa/manylinux2014_aarch64
#docker pull quay.io/pypa/manylinux2014_ppc64le
#docker pull quay.io/pypa/manylinux2014_s390x

docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux1_x86_64 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux1_i686 linux32 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_x86_64 /pymongo/.evergreen/build-wheels.sh
docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_i686 /pymongo/.evergreen/build-wheels.sh
#docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_aarch64 /pymongo/.evergreen/build-wheels.sh
#docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_ppc64le /pymongo/.evergreen/build-wheels.sh
#docker run --rm -v `pwd`:/pymongo quay.io/pypa/manylinux2014_s390x /pymongo/.evergreen/build-wheels.sh

ls dist
