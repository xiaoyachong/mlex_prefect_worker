#!/bin/bash

# Load environment variables from .env file
source .env

echo "Executing Folder: ${PWD}"
# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Start the worker command in the background, capture its PID, and assign the log file
(
    export PREFECT_WORK_DIR=$PREFECT_WORK_DIR
    export PYTHONPATH=$PWD:$PYTHONPATH
    prefect config set PREFECT_API_URL=$PREFECT_API_URL

    # Create logs directory if it doesn't exist
    mkdir -p logs

    # Create mlex pool for parent worker
    prefect work-pool create mlex_pool --type "process" || true
    prefect work-pool set-concurrency-limit mlex_pool $PREFECT_WORK_POOL_CONCURRENCY
    
    # Deploy all flows
    prefect deploy --all

    # Start the parent worker process, redirecting stdout and stderr to a temporary log file
    PREFECT_WORKER_WEBSERVER_PORT=8080 prefect worker start --pool mlex_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck &> "process_temp_parent.log" &
    pid_worker=$!

    # Rename the log file to include the actual PID of the worker process
    log_file="logs/parent_worker_${pid_worker}.log"
    mv "process_temp_parent.log" "$log_file"

    # Save the PID for easy reference
    echo "$pid_worker" > logs/parent_worker_pid.txt

    echo "Started parent Prefect worker with PID: $pid_worker and logging to $log_file"
    echo "To stop this worker, run: kill \$(cat logs/parent_worker_pid.txt)"
)