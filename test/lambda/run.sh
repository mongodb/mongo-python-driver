#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

sam build
sam local invoke --docker-network host --parameter-overrides "MongoDbUri=mongodb://host.docker.internal:27017"
