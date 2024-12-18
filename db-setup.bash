#!/bin/bash
source .env.test
source .env
set -e

if [ "$EUID" -ne 0 ]; then
    echo "This script must be run with sudo."
    exit 1
fi

echo "Building $DB_IMAGE image..."
docker build -f Dockerfile.db -t $DB_IMAGE . --no-cache

# Clear if the container is already running
if docker ps -a --format '{{.Names}}' | grep -q "^${DB_CONTAINER}$"; then
    echo "Container $DB_CONTAINER already exists, stopping and deleting..."
    docker stop $DB_CONTAINER
    docker rm $DB_CONTAINER
fi

# Check if the network already exists
if ! docker network inspect $DOCKER_NET &> /dev/null; then
    echo "Creating docker network $DOCKER_NET..."
    docker network create $DOCKER_NET
else
    echo "Using existing docker network $DOCKER_NET..."
fi

# Run the new container instance
echo "Starting $DB_CONTAINER container..."
docker run -d --env-file .env.test --network $DOCKER_NET --name $DB_CONTAINER -p $REDIS_PORT:$REDIS_PORT -p $TAOS_PORT:$TAOS_PORT -p $TAOS_HTTP_PORT:$TAOS_HTTP_PORT $DB_IMAGE

# Wait 5s for the db to start
sleep 5

# Test redis connection if redis-cli is locally installed, else warn user and print test command
if command -v redis-cli &> /dev/null; then
    echo "Testing local connection to Redis..."
    # Run command silently and check if it returns "PONG"
    output=$(redis-cli -h localhost -p $REDIS_PORT --user $DB_RW_USER --pass $DB_RW_PASS ping 2>/dev/null)
    if [ "$output" == "PONG" ]; then
        echo ">> Connection to redis successful"
    else
        echo ">> Connection to redis failed"
    fi
else
    echo "Warning: redis-cli not found, run the following command to test connection to redis:"
    echo "redis-cli -h localhost -p $REDIS_PORT --user $DB_RW_USER --pass $DB_RW_PASS ping"
fi

# Test tdengine connection if taos cli is locally installed, else warn user and print test command
if command -v taos &> /dev/null; then
    echo "Testing local connection to TDengine..."
    # Run command silently and check if it returns "2: service ok"
    output=$(taos -h localhost -P $TAOS_PORT -u $DB_RW_USER -p$DB_RW_PASS -k)
    if [[ "$output" == *"2: service ok"* ]]; then
        echo ">> Connection to TDengine successful"
    else
        echo ">> Connection to TDengine failed"
    fi
else
    echo "Warning: taos cli not found, run the following command to test connection to taos:"
    echo "taos -h localhost -P $TAOS_PORT -u $DB_RW_USER -p$DB_RW_PASS -k"
fi
