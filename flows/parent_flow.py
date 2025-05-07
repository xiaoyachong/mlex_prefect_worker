import logging
from enum import Enum

from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from prefect.states import Failed

# Import the Prefect client to check flow run states
from prefect.client import get_client

# Import schema classes for validation
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
def determine_best_environment(hpc_type: str) -> FlowType:
    """
    Determine the best execution environment based on hpc_type
    
    Args:
        hpc_type: Type of HPC to execute on
    
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
    elif hpc_type == "als":
        logger.info(f"HPC type is ALS cluster-ball, selecting DOCKER")
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
    flow_type: FlowType,
    params_list: list[dict],
):
    """
    Smart job router that automatically selects the best execution environment
    based on the HPC type.
    
    Args:
        flow_type: Not used--delete it later
        params_list: List of parameters for the job
    """
    prefect_logger = get_run_logger()
    client = get_client()
    
    # Hardcoded HPC type for now
    hpc_type = "als"
    prefect_logger.info(f"Starting job router (parent flow) for HPC: {hpc_type}")
    
    # Auto-select environment based on hpc_type
    target_env = determine_best_environment(hpc_type)
    prefect_logger.info(f"Selected target environment: {target_env}")
    
    # Execute each step in sequence based on the selected environment
    flow_run_id = ""
    
    for i, params in enumerate(params_list):
        prefect_logger.info(f"Running step {i+1} of {len(params_list)}")
        
        try:
            if target_env == FlowType.conda:
                # Extract only conda-relevant parameters
                conda_relevant_params = {
                    "conda_env_name": params["conda_env_name"],
                    "python_file_name": params["python_file_name"],
                    "params": params.get("params", {})
                }
                
                # Validate parameters with the schema
                conda_params = CondaParams(**conda_relevant_params)
                
                # If there's a previous flow run ID, set it in the parameters
                if flow_run_id:
                    if "io_parameters" not in conda_params.params:
                        conda_params.params["io_parameters"] = {}
                    conda_params.params["io_parameters"]["uid_retrieve"] = flow_run_id
                
                # Run the conda deployment with parameters
                deployment_data = {
                    "conda_params": conda_params.dict(),
                    "prev_flow_run_id": flow_run_id
                }
                flow_run = await run_deployment(
                    name="launch_conda/launch_conda",
                    parameters=deployment_data
                )
                
                # Check the status of the flow run
                flow_run = await client.read_flow_run(flow_run.id)
                
                if flow_run.state.is_failed():
                    prefect_logger.error(f"Step {i+1} failed with state: {flow_run.state.type}")
                    return Failed(message=f"Child flow failed with state: {flow_run.state.type}")
                    
                flow_run_id = str(flow_run.id)
                
            elif target_env == FlowType.docker:
                # Extract only docker-relevant parameters
                docker_relevant_params = {
                    "image_name": params["image_name"],
                    "image_tag": params["image_tag"],
                    "command": params.get("command", "python src/train.py"),
                    "volumes": params.get("volumes", []),
                    "network": params.get("network", ""),
                    "env_vars": params.get("env_vars", {}),
                    "params": params.get("params", {})
                }
                
                # Validate parameters with the schema
                docker_params = DockerParams(**docker_relevant_params)
                
                # If there's a previous flow run ID, set it in the parameters
                if flow_run_id:
                    if "io_parameters" not in docker_params.params:
                        docker_params.params["io_parameters"] = {}
                    docker_params.params["io_parameters"]["uid_retrieve"] = flow_run_id
                
                # Run the docker deployment with parameters
                deployment_data = {
                    "docker_params": docker_params.dict(),
                    "prev_flow_run_id": flow_run_id
                }
                flow_run = await run_deployment(
                    name="Docker flow/launch_docker",
                    parameters=deployment_data
                )
                
                # Check the status of the flow run
                flow_run = await client.read_flow_run(flow_run.id)
                
                if flow_run.state.is_failed():
                    prefect_logger.error(f"Step {i+1} failed with state: {flow_run.state.type}")
                    return Failed(message=f"Child flow failed with state: {flow_run.state.type}")
                    
                flow_run_id = str(flow_run.id)
                
            elif target_env == FlowType.podman:
                # Extract only podman-relevant parameters
                podman_relevant_params = {
                    "image_name": params["image_name"],
                    "image_tag": params["image_tag"],
                    "command": params.get("command", "python src/train.py"),
                    "volumes": params.get("volumes", []),
                    "network": params.get("network", ""),
                    "env_vars": params.get("env_vars", {}),
                    "params": params.get("params", {})
                }
                
                # Validate parameters with the schema
                podman_params = PodmanParams(**podman_relevant_params)
                
                # If there's a previous flow run ID, set it in the parameters
                if flow_run_id:
                    if "io_parameters" not in podman_params.params:
                        podman_params.params["io_parameters"] = {}
                    podman_params.params["io_parameters"]["uid_retrieve"] = flow_run_id
                
                # Run the podman deployment with parameters
                deployment_data = {
                    "podman_params": podman_params.dict(),
                    "prev_flow_run_id": flow_run_id
                }
                flow_run = await run_deployment(
                    name="Podman flow/launch_podman", 
                    parameters=deployment_data
                )
                
                # Check the status of the flow run
                flow_run = await client.read_flow_run(flow_run.id)
                
                if flow_run.state.is_failed():
                    prefect_logger.error(f"Step {i+1} failed with state: {flow_run.state.type}")
                    return Failed(message=f"Child flow failed with state: {flow_run.state.type}")
                    
                flow_run_id = str(flow_run.id)
                
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
                
                # Validate parameters with the schema
                slurm_params = SlurmParams(**slurm_relevant_params)
                
                # If there's a previous flow run ID, set it in the parameters
                if flow_run_id:
                    if "io_parameters" not in slurm_params.params:
                        slurm_params.params["io_parameters"] = {}
                    slurm_params.params["io_parameters"]["uid_retrieve"] = flow_run_id
                
                # Run the slurm deployment with parameters
                deployment_data = {
                    "slurm_params": slurm_params.dict(),
                    "prev_flow_run_id": flow_run_id
                }
                flow_run = await run_deployment(
                    name="launch_slurm/launch_slurm",
                    parameters=deployment_data
                )
                
                # Check the status of the flow run
                flow_run = await client.read_flow_run(flow_run.id)
                
                if flow_run.state.is_failed():
                    prefect_logger.error(f"Step {i+1} failed with state: {flow_run.state.type}")
                    return Failed(message=f"Child flow failed with state: {flow_run.state.type}")
                    
                flow_run_id = str(flow_run.id)
                
            else:
                prefect_logger.error("Flow type not supported")
                raise ValueError("Flow type not supported")
                
            prefect_logger.info(f"Step {i+1} completed with flow run ID: {flow_run_id}")
            
        except Exception as e:
            prefect_logger.error(f"Error in step {i+1}: {str(e)}")
            return Failed(message=f"Error in step {i+1}: {str(e)}")
    
    prefect_logger.info(f"All steps completed successfully. Final flow run ID: {flow_run_id}")
    return flow_run_id