import logging
import os
import json
from enum import Enum

import mlflow
from mlflow.tracking import MlflowClient
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

# MLflow connection parameters - load from environment variables
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "")
MLFLOW_TRACKING_USERNAME = os.getenv("MLFLOW_TRACKING_USERNAME", "")
MLFLOW_TRACKING_PASSWORD = os.getenv("MLFLOW_TRACKING_PASSWORD", "")

class FlowType(str, Enum):
    podman = "podman"
    conda = "conda"
    slurm = "slurm"
    docker = "docker"

@task
def determine_best_environment(hpc_type: str) -> FlowType:
    """
    Determine the best execution environment based on hpc_type.
    
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

@task
def get_algorithm_details_from_mlflow(model_name: str):
    """
    Retrieve algorithm details from MLflow using the model name.
    
    Args:
        model_name: The name of the model in MLflow
    
    Returns:
        Dictionary containing algorithm details
    """
    logger = get_run_logger()
    logger.info(f"Retrieving details for model {model_name} from MLflow")
    
    # Log MLflow connection parameters for debugging
    logger.info(f"MLflow Tracking URI: {MLFLOW_TRACKING_URI}")
    logger.info(f"MLflow Username: {'Set' if MLFLOW_TRACKING_USERNAME else 'Not set'}")
    logger.info(f"MLflow Password: {'Set' if MLFLOW_TRACKING_PASSWORD else 'Not set'}")
    
    # Set MLflow connection
    os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    try:
        client = MlflowClient()
        
        # Add additional logging to list all registered models
        try:
            all_models = client.search_registered_models()
            logger.info(f"Found {len(all_models)} registered models in MLflow:")
            for rm in all_models:
                logger.info(f"  - {rm.name}")
        except Exception as e:
            logger.warning(f"Could not list registered models: {str(e)}")
        
        # Get the latest version of the model
        logger.info(f"Attempting to get latest versions for model: {model_name}")
        versions = client.get_latest_versions(model_name)
        if not versions:
            logger.error(f"No versions found for model {model_name}")
            raise ValueError(f"Model {model_name} not found in MLflow")
            
        version = versions[0]
        logger.info(f"Found version {version.version} for model {model_name}")
        
        # Get the run to access parameters
        run = client.get_run(version.run_id)
        logger.info(f"Retrieved run with ID: {run.info.run_id}")
        
        # Extract the relevant parameters
        params = run.data.params
        tags = run.data.tags
        
        # Get relevant fields from MLflow params
        algorithm_details = {
            "model_name": model_name,
            "image_name": params.get("image_name", ""),
            "image_tag": params.get("image_tag", ""),
            "conda_env": params.get("conda_env", ""),
            "network": params.get("network", ""),
            "volumes": params.get("volumes", "[]"),
            "num_nodes": int(params.get("num_nodes", 1)),
            "partitions": params.get("partitions", "[]"),
            "reservations": params.get("reservations", "[]"),
            "max_time": params.get("max_time", "1:00:00"),
            "submission_ssh_key": params.get("submission_ssh_key", ""),
            "forward_ports": params.get("forward_ports", "[]"),
            "python_file": params.get("python_file", ""),
            "python_file_train": params.get("python_file_train", ""),
            "python_file_inference": params.get("python_file_inference", ""),
            "python_file_tune": params.get("python_file_tune", "")
        }
        
        logger.info(f"Successfully retrieved details for model {model_name}")
        return algorithm_details
        
    except Exception as e:
        logger.error(f"Error retrieving algorithm details from MLflow: {str(e)}")
        
        # Print the full exception traceback for better debugging
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@flow(name="Parent flow")
async def launch_parent_flow(params_list: list[dict]):
    """
    Smart job router that automatically selects the best execution environment
    based on the HPC type and loads algorithm details from MLflow.
    
    Args:
        params_list: List of parameters for the job, each containing model_name and task_name
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
    
    for i, child_job_params in enumerate(params_list):
        prefect_logger.info(f"Running step {i+1} of {len(params_list)}")
        
        try:
            # Get model name and task
            model_name = child_job_params.get("model_name", "")
            task_name = child_job_params.get("task_name", "")
            params = child_job_params.get("params", {})
            
            # Get algorithm details from MLflow
            algorithm_details = get_algorithm_details_from_mlflow(model_name)
            
            # Get the appropriate python file name based on the task name
            if task_name == "run":
                python_file = algorithm_details.get("python_file", "")
            elif task_name == "train":
                python_file = algorithm_details.get("python_file_train", "")
            elif task_name == "inference":
                python_file = algorithm_details.get("python_file_inference", "")
            elif task_name == "tune":
                python_file = algorithm_details.get("python_file_tune", "")
            else:
                # For any other task, default to python_file
                python_file = algorithm_details.get("python_file", "")
            
            if not python_file:
                prefect_logger.error(f"No Python file found for task {task_name}")
                raise ValueError(f"No Python file found for task {task_name}")
            
            if target_env == FlowType.conda:
                # Prepare conda parameters
                conda_relevant_params = {
                    "conda_env_name": algorithm_details["conda_env"],
                    "python_file_name": python_file,
                    "params": params
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
                # Prepare docker parameters
                docker_relevant_params = {
                    "image_name": algorithm_details["image_name"],
                    "image_tag": algorithm_details["image_tag"],
                    "command": f"python {python_file}",
                    "volumes": json.loads(algorithm_details["volumes"]),
                    "network": algorithm_details["network"],
                    "env_vars": {},
                    "params": params
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
                # Prepare podman parameters
                podman_relevant_params = {
                    "image_name": algorithm_details["image_name"],
                    "image_tag": algorithm_details["image_tag"],
                    "command": f"python {python_file}",
                    "volumes": json.loads(algorithm_details["volumes"]),
                    "network": algorithm_details["network"],
                    "env_vars": {},
                    "params": params
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
                # Prepare slurm parameters
                slurm_relevant_params = {
                    "job_name": f"{model_name}_{task_name}",
                    "num_nodes": algorithm_details["num_nodes"],
                    "partitions": json.loads(algorithm_details["partitions"]),
                    "reservations": json.loads(algorithm_details["reservations"]),
                    "max_time": algorithm_details["max_time"],
                    "conda_env_name": algorithm_details["conda_env"],
                    "forward_ports": json.loads(algorithm_details["forward_ports"]),
                    "submission_ssh_key": algorithm_details["submission_ssh_key"],
                    "python_file_name": python_file,
                    "params": params
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