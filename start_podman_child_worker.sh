#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# Create work pool for job type podman
prefect work-pool create podman_pool --type "process"
prefect work-pool set-concurrency-limit podman_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8082 prefect worker start --pool podman_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck

echo "Podman worker started"