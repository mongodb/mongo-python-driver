#!/bin/bash

if [ -n "${test_encryption}" ]; then
  bash .evergreen/setup-encryption.sh
fi
