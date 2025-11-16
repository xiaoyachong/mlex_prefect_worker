#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
export CONTAINER_WORK_DIR=$CONTAINER_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# Create docker type work pool
prefect work-pool create docker_pool --type "docker"
prefect work-pool update docker_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
yes n |prefect deploy -n launch_docker --pool docker_pool --prefect-file prefect-docker.yaml
PREFECT_WORKER_WEBSERVER_PORT=8081 prefect worker start --pool docker_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck

echo "Docker worker started"