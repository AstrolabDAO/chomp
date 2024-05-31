#!/bin/bash
# load setup test env
source .env.test

# Check dependencies
check_dependencies() {
    if ! command -v "$1" &> /dev/null; then
        echo "$2 is not installed or not running. Please install/start it and try again."
        exit 1
    else
        echo "$2 is installed, proceeding..."
    fi
}

# Check dependencies
check_dependencies docker "Docker"
check_dependencies python3 "Python3"
check_dependencies pdm "PDM"

# Install PDM if not installed
if ! command -v pdm &> /dev/null; then
    echo "PDM is not installed. Installing..."
    python3 -m $(command -v pip3 || command -v pip) install pdm
    echo "PDM installed successfully."
fi

# Set up backend
echo "Setting up chomp's back-end (redis+tdengine)..."
bash ./db-setup.bash

cd ..
# load runtime test env
source .env.test

# Install Chomp dependencies
echo "Installing dependencies..."
pdm install

# Start tiny Chomp ingestion cluster
echo "Starting up Chomp ingestion cluster ($TEST_INSTANCES instances)..."
for ((i = 1; i <= TEST_INSTANCES; i++)); do
    pdm run python main.py -v -e .env.test -c ./examples/oracle-feeds.yml -j 1 >/dev/null 2>&1 &
done

# Perpetual tail of out.log until interrupts
echo "Streaming outputs from ./out.log..."
tail -f ./out.log
