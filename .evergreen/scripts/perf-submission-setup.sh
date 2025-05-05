#!/bin/bash
# We use the requester expansion to determine whether the data is from a mainline evergreen run or not

set -eu

# shellcheck disable=SC2154
if [ "${requester}" == "commit" ]; then
  echo "is_mainline: true" >> expansion.yml
else
  echo "is_mainline: false" >> expansion.yml
fi

# We parse the username out of the order_id as patches append that in and SPS does not need that information
# shellcheck disable=SC2154
echo "parsed_order_id: $(echo "${revision_order_id}" | awk -F'_' '{print $NF}')"  >> expansion.yml
