# mlex_prefect_worker

This repository contains the necessary scripts and configuration files to start a Prefect process worker with conda, Docker, Podman and Slurm flows.

## Getting Started

### 1. Create and activate conda environment

Create a conda environment with the required packages:
```bash
conda create --name myenv python=3.11
conda activate myenv
```

### 2. Install the package

This will install dependencies:
```bash
python -m pip install .
```

### 3. Configure environment variables

Copy the example environment file and update it with your settings:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### 4. Make shell scripts executable

Change permissions for the shell scripts:
```bash
chmod +x start_parent_worker.sh
chmod +x start_docker_child_worker.sh
```

### 5. Start workers

#### Parent Worker (required)
The parent worker orchestrates job routing and execution:
```bash
./start_parent_worker.sh
```

#### Docker Child Worker (optional)
For Docker-based job execution:
```bash
./start_docker_child_worker.sh
```

## Worker Types

This repository supports multiple execution environments:

- **Conda**: Local execution in conda environments
- **Docker**: Containerized execution using Docker
- **Podman**: Containerized execution using Podman  
- **Slurm**: HPC cluster execution via Slurm scheduler

The parent worker automatically routes jobs to the appropriate execution environment based on the `worker.name` setting in `config.yml`.

## Configuration

Edit `config.yml` to configure:
- Worker type selection: facility names map to execution types (als→docker, nersc→slurm, nsls-ii→podman, conda→conda)
- Conda environment mappings
- Container volume mounts and networks
- Slurm job parameters

## Monitoring

Worker logs are stored in the `logs/` directory with the process ID in the filename:
```bash
# View Docker worker logs
tail -f logs/docker_worker_<pid>.log

# Stop Docker worker
kill $(cat logs/docker_worker_pid.txt)
```

## Copyright
MLExchange Copyright (c) 2024, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of
any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.
