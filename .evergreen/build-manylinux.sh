#!/bin/bash -ex

docker version

# manylinux1 2021-05-05-b64d921 and manylinux2014 2021-05-05-1ac6ef3 were
# the last releases to generate pip < 20.3 compatible wheels. After that
# auditwheel was upgraded to v4 which produces PEP 600 manylinux_x_y wheels
# which requires pip >= 20.3. We use the older docker image to support older
# pip versions.
BUILD_WITH_TAG="$1"
if [ -n "$BUILD_WITH_TAG" ]; then
  # 2020-03-20-2fda31c Was the last release to include Python 3.4.
  images=(quay.io/pypa/manylinux1_x86_64:2020-03-20-2fda31c \
          quay.io/pypa/manylinux1_i686:2020-03-20-2fda31c \
          quay.io/pypa/manylinux1_x86_64:2021-05-05-b64d921 \
          quay.io/pypa/manylinux1_i686:2021-05-05-b64d921 \
          quay.io/pypa/manylinux2014_x86_64:2021-05-05-1ac6ef3 \
          quay.io/pypa/manylinux2014_i686:2021-05-05-1ac6ef3 \
          quay.io/pypa/manylinux2014_aarch64:2021-05-05-1ac6ef3 \
          quay.io/pypa/manylinux2014_ppc64le:2021-05-05-1ac6ef3 \
          quay.io/pypa/manylinux2014_s390x:2021-05-05-1ac6ef3)
else
  images=(quay.io/pypa/manylinux1_x86_64 \
          quay.io/pypa/manylinux1_i686 \
          quay.io/pypa/manylinux2014_x86_64 \
          quay.io/pypa/manylinux2014_i686 \
          quay.io/pypa/manylinux2014_aarch64 \
          quay.io/pypa/manylinux2014_ppc64le \
          quay.io/pypa/manylinux2014_s390x)
fi

for image in "${images[@]}"; do
  docker pull $image
  docker run --rm -v `pwd`:/src $image /src/.evergreen/build-manylinux-internal.sh
done

ls dist

# Check for any unexpected files.
unexpected=$(find dist \! \( -iname dist -or \
                             -iname '*cp27*' -or \
                             -iname '*cp34*' -or \
                             -iname '*cp35*' -or \
                             -iname '*cp36*' -or \
                             -iname '*cp37*' -or \
                             -iname '*cp38*' -or \
                             -iname '*cp39*' \))
if [ -n "$unexpected" ]; then
  echo "Unexpected files:" $unexpected
  exit 1
fi
