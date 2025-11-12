import logging
import os
import json
from enum import Enum

import yaml
import mlflow
from mlflow.tracking import MlflowClient
from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from prefect.states import Failed
from dotenv import load_dotenv  # Add this import

# Import the Prefect client to check flow run states
from prefect.client import get_client

# Import schema classes for validation
from flows.conda.schema import CondaParams
from flows.docker.schema import DockerParams
from flows.podman.schema import PodmanParams
from flows.slurm.schema import SlurmParams

# Load .env file at module import
load_dotenv()  # Add this line

logger = logging.getLogger(__name__)

# MLflow connection parameters - load from environment variables
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "")
MLFLOW_TRACKING_USERNAME = os.getenv("MLFLOW_TRACKING_USERNAME", "")
MLFLOW_TRACKING_PASSWORD = os.getenv("MLFLOW_TRACKING_PASSWORD", "")

# Path to configuration file
CONFIG_PATH = "config.yml"

class FlowType(str, Enum):
    podman = "podman"
    conda = "conda"
    slurm = "slurm"
    docker = "docker"

def expand_env_vars(obj):
    """Recursively expand environment variables in nested structures"""
    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [expand_env_vars(item) for item in obj]
    elif isinstance(obj, str):
        # Expand ${VAR} and $VAR patterns
        return os.path.expandvars(obj)
    else:
        return obj

def load_config():
    """
    Load the configuration from config.yml file and expand environment variables.
    
    Returns:
        Dictionary containing configuration with expanded env vars
    """
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        # Expand all environment variables
        config = expand_env_vars(config)
        
        return config
    except Exception as e:
        logger.error(f"Error loading configuration from {CONFIG_PATH}: {str(e)}")
        return {}

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

def _get_conda_env_for_model(model_name: str, config: dict) -> str:
    """
    Simple helper function to determine the appropriate conda environment for a model.
    
    Args:
        model_name: The name of the model
        config: The configuration dictionary from config.yml
        
    Returns:
        The appropriate conda environment name
    """
    conda_envs = config.get("conda", {}).get("conda_env_name", {})
    
    # Determine model type from the model name
    if "dlsia" in model_name.lower():
        return conda_envs.get("dlsia", "")
    elif "autoencoder" in model_name.lower():
        return conda_envs.get("pytorch_autoencoder", "")
    elif "pca" in model_name.lower():
        return conda_envs.get("pca", "")
    elif "umap" in model_name.lower():
        return conda_envs.get("umap", "")
    elif any(cluster_type in model_name.lower() for cluster_type in ["cluster", "dbscan", "hdbscan", "kmeans"]):
        return conda_envs.get("clustering", "")
    
    # Default to first conda environment if available
    if conda_envs:
        return next(iter(conda_envs.values()))
    return ""

@task
def get_algorithm_details_from_mlflow(model_name: str, config: dict):
    """
    Retrieve algorithm details from MLflow using the model name.
    
    Args:
        model_name: The name of the model in MLflow
        config: Configuration dictionary from config.yml
    
    Returns:
        Tuple containing (algorithm_details, job_details)
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
        
        # Get algorithm details from MLflow - only the core information
        algorithm_details = {
            "model_name": model_name,
            # Core Algorithm Information
            "image_name": params.get("image_name", ""),
            "image_tag": params.get("image_tag", ""),
            "source": params.get("source", ""),
            "is_gpu_enabled": params.get("is_gpu_enabled", "False").lower() == "true"
        }
        
        # Handle Python file paths
        if "python_file_train" in params:
            algorithm_details["python_file_train"] = params.get("python_file_train", "")
        if "python_file_inference" in params:
            algorithm_details["python_file_inference"] = params.get("python_file_inference", "")
        if "python_file_tune" in params:
            algorithm_details["python_file_tune"] = params.get("python_file_tune", "")
        if "python_file" in params:
            algorithm_details["python_file"] = params.get("python_file", "")
        
        # Create job details from config.yml (already expanded by load_config)
        job_details = {
            # Container settings
            "volumes": config.get("container", {}).get("volumes", []),
            "network": config.get("container", {}).get("network", ""),
            # Slurm settings
            "num_nodes": config.get("slurm", {}).get("num_nodes", 1),
            "partitions": config.get("slurm", {}).get("partitions", "[]"),
            "reservations": config.get("slurm", {}).get("reservations", "[]"),
            "max_time": config.get("slurm", {}).get("max_time", "1:00:00"),
            "submission_ssh_key": config.get("slurm", {}).get("submission_ssh_key", ""),
            "forward_ports": config.get("slurm", {}).get("forward_ports", "[]"),
            # Get conda environment based on the model type
            "conda_env": _get_conda_env_for_model(model_name, config)
        }
        
        logger.info(f"Successfully retrieved details for model {model_name}")
        return algorithm_details, job_details
        
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
    
    # Load configuration from file (with env vars expanded)
    config = load_config()
    
    # Get HPC type from config, default to "als" if not specified
    hpc_type = config.get("hpc_type", "als")
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
            
            # Get algorithm details and job details from MLflow
            algorithm_details, job_details = get_algorithm_details_from_mlflow(model_name, config)
            
            # Get the appropriate python file name based on the task name
            if task_name == "execute":
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
                # Prepare conda parameters - use job_details for conda_env
                conda_relevant_params = {
                    "conda_env_name": job_details["conda_env"],
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
                # Prepare docker parameters - use algorithm_details for image info and job_details for environment
                docker_relevant_params = {
                    "image_name": algorithm_details["image_name"],
                    "image_tag": algorithm_details["image_tag"],
                    "command": f"python {python_file}",
                    "volumes": job_details["volumes"],
                    "network": job_details["network"],
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
                # Prepare podman parameters - use algorithm_details for image info and job_details for environment
                podman_relevant_params = {
                    "image_name": algorithm_details["image_name"],
                    "image_tag": algorithm_details["image_tag"],
                    "command": f"python {python_file}",
                    "volumes": job_details["volumes"],
                    "network": job_details["network"],
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
                # Parse string JSON values if needed
                partitions = job_details["partitions"]
                if isinstance(partitions, str):
                    partitions = json.loads(partitions)
                
                reservations = job_details["reservations"]
                if isinstance(reservations, str):
                    reservations = json.loads(reservations)
                
                forward_ports = job_details["forward_ports"]
                if isinstance(forward_ports, str):
                    forward_ports = json.loads(forward_ports)
                
                # Prepare slurm parameters - use job_details for slurm configuration
                slurm_relevant_params = {
                    "job_name": f"{model_name}_{task_name}",
                    "num_nodes": job_details["num_nodes"],
                    "partitions": partitions,
                    "reservations": reservations,
                    "max_time": job_details["max_time"],
                    "conda_env_name": job_details["conda_env"],
                    "forward_ports": forward_ports,
                    "submission_ssh_key": job_details["submission_ssh_key"],
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