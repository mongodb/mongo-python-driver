#!/bin/bash -ex

docker version

# 2020-03-20-2fda31c Was the last release to include Python 3.4.
images=(quay.io/pypa/manylinux1_x86_64:2020-03-20-2fda31c \
        quay.io/pypa/manylinux1_i686:2020-03-20-2fda31c \
        quay.io/pypa/manylinux1_x86_64 \
        quay.io/pypa/manylinux1_i686 \
        quay.io/pypa/manylinux2014_x86_64 \
        quay.io/pypa/manylinux2014_i686)
# aarch64/ppc64le/s390x work on macOS locally but not on linux in evergreen:
# [2020/07/23 00:24:00.482] + docker run --rm -v /data/mci/cd100cec6341abda533450fb3f2fab99/src:/pymongo quay.io/pypa/manylinux2014_aarch64 /pymongo/.evergreen/build-wheels.sh
# [2020/07/23 00:24:01.186] standard_init_linux.go:211: exec user process caused "exec format error"
#
# Could be related to:
# https://github.com/pypa/manylinux/issues/410
#        quay.io/pypa/manylinux2014_aarch64 \
#        quay.io/pypa/manylinux2014_ppc64le \
#        quay.io/pypa/manylinux2014_s390x)

for image in "${images[@]}"; do
  docker pull $image
  docker run --rm -v `pwd`:/pymongo $image /pymongo/.evergreen/build-wheels.sh
done

ls dist

# Check for any unexpected files.
unexpected=$(find dist \! \( -iname dist -or \
                             -iname '*cp27*' -or \
                             -iname '*cp34*' -or \
                             -iname '*cp35*' -or \
                             -iname '*cp36*' -or \
                             -iname '*cp37*' -or \
                             -iname '*cp38*' \))
if [ -n "$unexpected" ]; then
  echo "Unexpected files:" $unexpected
  exit 1
fi
