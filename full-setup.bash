#!/bin/bash
source .env.test
source .env
set -e

# Check dependencies function
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
# check_dependencies python3 "Python3"
# check_dependencies pdm "PDM"

# Install PDM if not installed
# if ! command -v pdm &> /dev/null; then
#     echo "PDM is not installed. Installing..."
#     python3 -m $(command -v pip3 || command -v pip) install pdm
#     echo "PDM installed successfully."
# fi

# Set up backend
echo "Setting up chomp's back-end (redis+tdengine)..."
bash ./db-setup.bash

# Set up core
echo "Setting up chomp's core (API+ingesters)..."
bash ./core-setup.bash
