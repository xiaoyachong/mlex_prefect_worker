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

    # Create log directory if it doesn't exist
    mkdir -p logs

    # 1. Create docker worker pool
    prefect work-pool create docker_pool --type "process" || true
    prefect work-pool update docker_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
    prefect deploy -n launch_docker --pool docker_pool
    
    # Start the docker worker with logs that include PID
    PREFECT_WORKER_WEBSERVER_PORT=8081 prefect worker start --pool docker_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck > "process_temp_docker.log" 2>&1 &
    docker_pid=$!
    # Rename the log file to include the actual PID of the worker process
    docker_log="logs/docker_worker_${docker_pid}.log"
    mv "process_temp_docker.log" "$docker_log"
    echo "Started Docker worker with PID: $docker_pid and logging to $docker_log"

    # 2. Create podman worker pool
    prefect work-pool create podman_pool --type "process" || true
    prefect work-pool update podman_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
    prefect deploy -n launch_podman --pool podman_pool
    
    # Start the podman worker with logs that include PID
    PREFECT_WORKER_WEBSERVER_PORT=8082 prefect worker start --pool podman_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck > "process_temp_podman.log" 2>&1 &
    podman_pid=$!
    # Rename the log file to include the actual PID of the worker process
    podman_log="logs/podman_worker_${podman_pid}.log"
    mv "process_temp_podman.log" "$podman_log"
    echo "Started Podman worker with PID: $podman_pid and logging to $podman_log"

    # 3. Create conda worker pool
    prefect work-pool create conda_pool --type "process" || true
    prefect work-pool update conda_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
    prefect deploy -n launch_conda --pool conda_pool
    
    # Start the conda worker with logs that include PID
    PREFECT_WORKER_WEBSERVER_PORT=8083 prefect worker start --pool conda_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck > "process_temp_conda.log" 2>&1 &
    conda_pid=$!
    # Rename the log file to include the actual PID of the worker process
    conda_log="logs/conda_worker_${conda_pid}.log"
    mv "process_temp_conda.log" "$conda_log"
    echo "Started Conda worker with PID: $conda_pid and logging to $conda_log"

    # 4. Create slurm worker pool
    prefect work-pool create slurm_pool --type "process" || true
    prefect work-pool update slurm_pool --concurrency-limit $PREFECT_WORK_POOL_CONCURRENCY
    prefect deploy -n launch_slurm --pool slurm_pool
    
    # Start the slurm worker with logs that include PID
    PREFECT_WORKER_WEBSERVER_PORT=8084 prefect worker start --pool slurm_pool --limit $PREFECT_WORKER_LIMIT --with-healthcheck > "process_temp_slurm.log" 2>&1 &
    slurm_pid=$!
    # Rename the log file to include the actual PID of the worker process
    slurm_log="logs/slurm_worker_${slurm_pid}.log"
    mv "process_temp_slurm.log" "$slurm_log"
    echo "Started Slurm worker with PID: $slurm_pid and logging to $slurm_log"

    echo "All child workers started in background"
    
    # Create a pid file with all workers for easy termination later
    echo "$docker_pid $podman_pid $conda_pid $slurm_pid" > logs/child_workers_pids.txt
    echo "PIDs saved to logs/child_workers_pids.txt for future reference"
    
    # Output summary 
    echo ""
    echo "===== Child Workers Summary ====="
    echo "Docker worker: PID $docker_pid, Port 8081, Log: $docker_log"
    echo "Podman worker: PID $podman_pid, Port 8082, Log: $podman_log"
    echo "Conda worker: PID $conda_pid, Port 8083, Log: $conda_log"
    echo "Slurm worker: PID $slurm_pid, Port 8084, Log: $slurm_log"
    echo "=============================="
    echo ""
    echo "To view logs, use: tail -f $docker_log"
    echo "To stop workers, run: kill \$(cat logs/child_workers_pids.txt)"
)