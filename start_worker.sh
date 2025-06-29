source .env
export MLFLOW_TRACKING_URI
export MLFLOW_TRACKING_USERNAME
export MLFLOW_TRACKING_PASSWORD

export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
prefect config set PREFECT_API_URL=$PREFECT_API_URL

prefect work-pool create mlex_pool --type "process"
prefect work-pool set-concurrency-limit mlex_pool $PREFECT_WORK_POOL_CONCURRENCY
prefect deploy --all

prefect worker start --pool mlex_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck