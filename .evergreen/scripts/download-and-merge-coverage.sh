#!/bin/bash

set +x
# Download all the task coverage files.
aws s3 cp --recursive s3://"$1"/coverage/"$2"/"$3"/coverage/ coverage/
