#!/bin/bash
source .env.test

docker build -f Dockerfile.dbs -t collector-dbs .
docker run -d -p ${KDB_PORT}:${KDB_PORT} -p ${REDIS_PORT}:${REDIS_PORT} --env-file .env.test collector-dbs
