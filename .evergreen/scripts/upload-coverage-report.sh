#!/bin/bash
# Upload a coverate report to s3.
set -eu
aws s3 cp htmlcov/ s3://"$1"/coverage/"$2"/"$3"/htmlcov/ --recursive --acl public-read --region us-east-1
