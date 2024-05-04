#!/bin/bash
source .env.test

docker build -f Dockerfile.dbs -t collector-dbs .

# Check if the container is already running
if docker ps -a --format '{{.Names}}' | grep -q "^${DB_CONTAINER}$"; then
    echo "Container $DB_CONTAINER already exists, stopping and deleting..."
    # Stop the container if it's running
    docker stop $DB_CONTAINER
    # Remove the container
    docker rm $DB_CONTAINER
fi

# Run the new container instance
echo "Starting a new container $DB_CONTAINER..."
docker run -d --name $DB_CONTAINER collector-dbs
