#!/bin/bash
source .env

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

prefect work-pool create parent_pool --type "process"
prefect work-pool update parent_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY

prefect deploy -n launch_parent_flow --pool parent_pool 
prefect worker start --pool parent_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck