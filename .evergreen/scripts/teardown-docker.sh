#!/bin/bash

# Remove all Docker images
DOCKER=$(command -v docker) || true
if [ -n "$DOCKER" ]; then
  docker rmi -f "$(docker images -a -q)" &> /dev/null || true
fi
