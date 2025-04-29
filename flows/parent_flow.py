import logging
from typing import Dict, Any, List, Optional, Union
from enum import Enum

import asyncio
from prefect import flow, task, get_run_logger

# Import existing flow implementations
from flows.conda.conda_flows import launch_conda
from flows.docker.docker_flows import launch_docker
from flows.podman.podman_flows import launch_podman
from flows.slurm.slurm_flows import launch_slurm
from flows.conda.schema import CondaParams
from flows.docker.schema import DockerParams
from flows.podman.schema import PodmanParams
from flows.slurm.schema import SlurmParams

logger = logging.getLogger(__name__)

class FlowType(str, Enum):
    podman = "podman"
    conda = "conda"
    slurm = "slurm"
    docker = "docker"

@task
def determine_best_environment(hpc_type: str, params_list: list[dict]) -> FlowType:
    """
    Determine the best execution environment based on hpc_type
    
    Args:
        hpc_type: Type of HPC to execute on
        params_list: List of job parameters
    
    Returns:
        Best flow type to use
    """
    logger = get_run_logger()
    
    # Map HPC type to flow type
    hpc_type = hpc_type.lower()
    if hpc_type == "nersc":
        logger.info(f"HPC type is NERSC, selecting SLURM")
        return FlowType.slurm
    elif hpc_type == "nsls-ii":
        logger.info(f"HPC type is NSLS-II, selecting PODMAN")
        return FlowType.podman
    elif hpc_type == "als" or hpc_type == "cluster-ball":
        logger.info(f"HPC type is ALS/cluster-ball, selecting DOCKER")
        return FlowType.docker
    elif hpc_type in [ft.value for ft in FlowType]:
        # If the hpc_type is actually a flow type, use it directly
        return FlowType(hpc_type)
    else:
        # Default to conda for unknown HPC types
        logger.info(f"Unknown HPC type: {hpc_type}, defaulting to CONDA environment")
        return FlowType.conda

@flow(name="Parent flow")
async def launch_parent_flow(
    hpc_type: str,
    params_list: list[dict],
):
    """
    Smart job router that automatically selects the best execution environment
    based on the HPC type.
    
    Args:
        hpc_type: Type of HPC to execute on
        params_list: List of parameters for the job
    """
    prefect_logger = get_run_logger()
    prefect_logger.info(f"Starting job router (parent flow) for HPC: {hpc_type}")
    
    # Auto-select environment based on hpc_type
    target_env = determine_best_environment(hpc_type, params_list)
    prefect_logger.info(f"Selected target environment: {target_env}")
    
    # Execute each step in sequence based on the selected environment
    flow_run_id = ""
    
    for i, params in enumerate(params_list):
        if target_env == FlowType.conda:
            # Extract only conda-relevant parameters
            conda_relevant_params = {
                "conda_env_name": params["conda_env_name"],
                "python_file_name": params["python_file_name"],
                "params": params.get("params", {})
            }
            # Create CondaParams object with only relevant parameters
            conda_params = CondaParams(**conda_relevant_params)
            flow_run_id = await launch_conda(
                conda_params=conda_params,
                prev_flow_run_id=flow_run_id
            )
            
        elif target_env == FlowType.docker:
            # Extract only docker-relevant parameters
            docker_relevant_params = {
                "image_name": params["image_name"],
                "image_tag": params["image_tag"],
                "command": params.get("command", "python src/train.py"),
                "volumes": params.get("volumes", []),
                "network": params.get("network", ""),
                "params": params.get("params", {})
            }
            # Create DockerParams object with only relevant parameters
            docker_params = DockerParams(**docker_relevant_params)
            flow_run_id = await launch_docker(
                docker_params=docker_params,
                prev_flow_run_id=flow_run_id
            )
            
        elif target_env == FlowType.podman:
            # Extract only podman-relevant parameters
            podman_relevant_params = {
                "image_name": params["image_name"],
                "image_tag": params["image_tag"],
                "command": params.get("command", "python src/train.py"),
                "volumes": params.get("volumes", []),
                "network": params.get("network", ""),
                "params": params.get("params", {})
            }
            # Create PodmanParams object with only relevant parameters
            podman_params = PodmanParams(**podman_relevant_params)
            flow_run_id = await launch_podman(
                podman_params=podman_params,
                prev_flow_run_id=flow_run_id
            )
            
        elif target_env == FlowType.slurm:
            # Extract only slurm-relevant parameters
            slurm_relevant_params = {
                "job_name": params["job_name"],
                "num_nodes": params["num_nodes"],
                "partitions": params.get("partitions", []),
                "reservations": params.get("reservations", []),
                "max_time": params["max_time"],
                "conda_env_name": params["conda_env_name"],
                "forward_ports": params.get("forward_ports", []),
                "submission_ssh_key": params.get("submission_ssh_key", None),
                "python_file_name": params.get("python_file_name", "src/train.py"),
                "params": params.get("params", {})
            }
            # Create SlurmParams object with only relevant parameters
            slurm_params = SlurmParams(**slurm_relevant_params)
            flow_run_id = await launch_slurm(
                slurm_params=slurm_params,
                prev_flow_run_id=flow_run_id
            )
            
        else:
            prefect_logger.error("Flow type not supported")
            raise ValueError("Flow type not supported")
    
    pass