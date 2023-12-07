#!/bin/bash

source ./secrets-export.sh

{
  echo "export AWS_ACCESS_KEY_ID=$FLE_AWS_KEY";
  echo "export AWS_SECRET_ACCESS_KEY=$FLE_AWS_SECRET";
  echo "export AWS_DEFAULT_REGION=us-east-1";
  echo "export AWS_SESSION_TOKEN=";
} >> ./secrets-export.sh
