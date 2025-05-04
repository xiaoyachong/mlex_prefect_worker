#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# 1. Create work pools for job type docker 
prefect work-pool create docker_pool --type "process"
prefect work-pool set-concurrency-limit docker_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8081 prefect worker start --pool docker_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 2. Create work pools for job type docker
prefect work-pool create podman_pool --type "process"
prefect work-pool set-concurrency-limit podman_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8082 prefect worker start --pool podman_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 3. Create work pools for job type conda
prefect work-pool create conda_pool --type "process"
prefect work-pool set-concurrency-limit conda_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8083 prefect worker start --pool conda_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 4. Create work pools for job type slurm
prefect work-pool create slurm_pool --type "process"
prefect work-pool set-concurrency-limit slurm_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8084 prefect worker start --pool slurm_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

echo "All workers started"
wait