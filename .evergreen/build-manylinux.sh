#!/bin/bash -ex

docker version

# 2020-03-20-2fda31c Was the last release to include Python 3.4.
images=(quay.io/pypa/manylinux1_x86_64:2020-03-20-2fda31c \
        quay.io/pypa/manylinux1_i686:2020-03-20-2fda31c \
        quay.io/pypa/manylinux1_x86_64 \
        quay.io/pypa/manylinux1_i686 \
        quay.io/pypa/manylinux2014_x86_64 \
        quay.io/pypa/manylinux2014_i686 \
        quay.io/pypa/manylinux2014_aarch64 \
        quay.io/pypa/manylinux2014_ppc64le \
        quay.io/pypa/manylinux2014_s390x)

for image in "${images[@]}"; do
  docker pull $image
  docker run --rm -v `pwd`:/pymongo $image /pymongo/.evergreen/build-manylinux-internal.sh
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
