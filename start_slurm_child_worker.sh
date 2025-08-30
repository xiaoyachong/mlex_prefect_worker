#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# Create work pool for job type slurm
prefect work-pool create slurm_pool --type "process"
prefect work-pool set-concurrency-limit slurm_pool $PREFECT_WORK_POOL_CONCURRENCY
PREFECT_WORKER_WEBSERVER_PORT=8084 prefect worker start --pool slurm_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck

echo "Slurm worker started"