#!/bin/bash

# Run from the root of the project directory

set -e

docker compose -f test-deps/docker-compose.yaml down
docker compose -f test-deps/docker-compose.yaml up -d

echo Waiting for Tag Manager API to become available...
curl --fail --silent --show-error \
            --retry 10 --retry-delay 2 --retry-max-time 30 --retry-all-errors \
            -H 'X-API-Key: TEST_KEY' \
            http://localhost:8080/custom/tags
echo Tag Manager API is available.

pytest $@

docker compose -f test-deps/docker-compose.yaml down
# Containers only shut down if tests succeed, otherwise they remain running for debugging.
