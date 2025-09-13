#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# 1. Docker pool
prefect work-pool create docker_pool --type "process"
prefect work-pool update docker_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy -n launch_docker --pool docker_pool 
PREFECT_WORKER_WEBSERVER_PORT=8081 prefect worker start --pool docker_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 2. Podman pool
prefect work-pool create podman_pool --type "process"
prefect work-pool update podman_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy -n launch_podman --pool podman_pool  
PREFECT_WORKER_WEBSERVER_PORT=8082 prefect worker start --pool podman_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 3. Conda pool
prefect work-pool create conda_pool --type "process"
prefect work-pool update conda_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy -n launch_conda --pool conda_pool 
PREFECT_WORKER_WEBSERVER_PORT=8083 prefect worker start --pool conda_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

# 4. Slurm pool
prefect work-pool create slurm_pool --type "process"
prefect work-pool update slurm_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy -n launch_slurm --pool slurm_pool 
PREFECT_WORKER_WEBSERVER_PORT=8084 prefect worker start --pool slurm_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &

echo "All workers started"
wait