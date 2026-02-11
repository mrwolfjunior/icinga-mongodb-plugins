#!/bin/bash
# Helper script to run E2E tests from the host machine using a temporary Docker container.
# This avoids hostname resolution issues by running pytest inside the Docker network.

if [ "$1" == "--help" ]; then
    echo "Usage: ./run_e2e.sh [pytest_args]"
    echo "Example: ./run_e2e.sh -v -k test_availability_quorum_lost"
    exit 0
fi

# Ensure the network exists (created by docker-compose)
if ! docker network inspect docker_mongonet >/dev/null 2>&1; then
    echo "Error: Network 'docker_mongonet' not found. Start the stack first:"
    echo "docker compose -f docker/docker-compose.replicaset.yml up -d"
    exit 1
fi

echo "Running E2E tests in Docker container..."
docker run --rm \
    --network docker_mongonet \
    -v "$(pwd):/app" \
    -w /app \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e RS_URI="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0" \
    -e RS_ARBITER_URI="mongodb://mongo-data1:27017,mongo-data2:27017,mongo-arbiter:27017/?replicaSet=rsArbiter" \
    -e SHARDED_URI="mongodb://mongos1:27017,mongos2:27017/" \
    python:3.9-slim \
    bash -c "apt-get update >/dev/null && apt-get install -y docker.io >/dev/null && pip install -q pymongo pytest && python -m pytest tests/test_e2e.py $@"
