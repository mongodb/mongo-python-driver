#!/bin/bash -ex

docker version

images=(quay.io/pypa/manylinux1_x86_64 \
        quay.io/pypa/manylinux1_i686 \
        quay.io/pypa/manylinux2014_x86_64 \
        quay.io/pypa/manylinux2014_i686 \
        quay.io/pypa/manylinux2014_aarch64 \
        quay.io/pypa/manylinux2014_ppc64le \
        quay.io/pypa/manylinux2014_s390x)

for image in "${images[@]}"; do
  docker pull $image
  docker run --rm -v `pwd`:/src $image /src/.evergreen/build-manylinux-internal.sh
done

ls dist

# Check for any unexpected files.
unexpected=$(find dist \! \( -iname dist -or \
                             -iname '*cp35*' -or \
                             -iname '*cp36*' -or \
                             -iname '*cp37*' -or \
                             -iname '*cp38*' -or \
                             -iname '*cp39*' \))
if [ -n "$unexpected" ]; then
  echo "Unexpected files:" $unexpected
  exit 1
fi
