#!/bin/bash

if [ -n "${test_encryption}" ]; then
  ./.evergreen/hatch.sh encryption:setup
fi
