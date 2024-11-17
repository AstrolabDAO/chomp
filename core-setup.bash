#!/bin/bash
source .env.test
source .env
set -e

if [ "$EUID" -ne 0 ]; then
    echo "This script must be run with sudo."
    exit 1
fi

# Build Docker image for API and ingester
echo "Building Chomp Docker images ($API_IMAGE, $INGESTER_IMAGE, $HEALTHCHECK_IMAGE)..."
docker build -f Dockerfile.core -t $API_IMAGE . --no-cache
docker build -f Dockerfile.core -t $INGESTER_IMAGE . --no-cache
docker build -f Dockerfile.core -t $HEALTHCHECK_IMAGE . --no-cache

# Delete existing containers
containers=("$DB_CONTAINER" "$API_CONTAINER" "$INGESTER_CONTAINER" "$HEALTHCHECK_CONTAINER")
for container in "${containers[@]}"; do
    # Stop and remove the container if it exists
    if docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
        echo "Stopping and removing container: $container"
        docker stop "$container" && docker rm "$container"
    else
        echo "Container $container does not exist."
    fi
done

# Health checks
echo "Health checking..."
docker run --env-file .env.test --network $DOCKER_NET --name $HEALTHCHECK_CONTAINER $HEALTHCHECK_IMAGE -v -e .env.test -c $CONFIG_PATH --ping
EXIT_CODE=$?

# Check the exit code and act accordingly
if [ $EXIT_CODE -ne 0 ]; then
    echo "Health check $HEALTHCHECK_CONTAINER failed with code: $EXIT_CODE, db or cache connection issue"
    exit $EXIT_CODE
else
    echo "Health check $HEALTHCHECK_CONTAINER successful: db and cache connection established"
fi

# Start ingestion instances
echo "Starting Chomp ingestion cluster ($CLUSTER_INSTANCES instances)..."
for ((i = 1; i <= CLUSTER_INSTANCES; i++)); do
    docker run -d --env-file .env.test --network $DOCKER_NET --name $INGESTER_CONTAINER-$i $INGESTER_IMAGE -v -e .env.test -c $CONFIG_PATH -j 2 
done

# Start server node
echo "Starting Chomp server node..."
docker run -d --env-file .env.test --network $DOCKER_NET --name $API_CONTAINER $API_IMAGE -v -s -e .env.test -c $CONFIG_PATH 

# Perpetual tail of containers.log until interrupts
echo "Streaming outputs from ./containers.log..."
tail -f ./containers.log
