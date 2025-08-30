#!/bin/bash
source .env
export MLFLOW_TRACKING_URI
export MLFLOW_TRACKING_USERNAME
export MLFLOW_TRACKING_PASSWORD

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

prefect work-pool create parent_pool --type "process"
prefect work-pool set-concurrency-limit parent_pool $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy --all

prefect worker start --pool parent_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck