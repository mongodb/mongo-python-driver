#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

sam build
sam local invoke --parameter-overrides "MongoDbUri=mongodb://192.168.65.1:27017"
