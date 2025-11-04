import tempfile
import os
import subprocess
import yaml
from prefect import context, flow
from prefect.states import Failed

from flows.docker.schema import DockerParams
from flows.logger import setup_logger


@flow(name="Docker flow")
def launch_docker(
    docker_params: DockerParams,
    prev_flow_run_id: str = "",
):
    logger = setup_logger()

    if (
        prev_flow_run_id != ""
        and docker_params.params["io_parameters"]["uid_retrieve"] == ""
    ):
        # Append the previous flow run id to parameters if provided
        docker_params.params["io_parameters"]["uid_retrieve"] = prev_flow_run_id

    current_flow_run_id = str(context.get_run_context().flow_run.id)

    # Append current flow run id
    docker_params.params["io_parameters"]["uid_save"] = current_flow_run_id

    # Create temporary file in the mounted directory so it's accessible from host
    temp_dir = "/mlex_prefect_worker/tmp"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create temporary file for parameters
    temp_file_fd, temp_path = tempfile.mkstemp(suffix=".yaml", dir=temp_dir, text=True)
    try:
        with os.fdopen(temp_file_fd, 'w') as f:
            yaml.dump(docker_params.params, f)
        
        logger.info(f"Parameters file: {temp_path}")
        
        # Convert container path to host path for Docker volume mounting
        # /mlex_prefect_worker/tmp/xyz.yaml -> /Users/xiaoyachong/Documents/3RSE/mlex_prefect_worker/tmp/xyz.yaml
        host_temp_path = temp_path.replace("/mlex_prefect_worker", "/Users/xiaoyachong/Documents/3RSE/mlex_prefect_worker")

        # Build docker command directly (no bash script needed)
        docker_cmd = ["docker", "run", "--rm"]
        
        # Add volumes
        volumes = docker_params.volumes + [f"{host_temp_path}:/app/work/config/params.yaml"]
        for volume in volumes:
            docker_cmd.extend(["-v", volume])
        
        # Add network
        if docker_params.network:
            docker_cmd.extend(["--network", docker_params.network])
        
        # Add env vars
        for key, value in docker_params.env_vars.items():
            docker_cmd.extend(["-e", f"{key}={value}"])
        
        # Add image
        docker_cmd.append(f"{docker_params.image_name}:{docker_params.image_tag}")
        
        # Add command with params file path
        command_parts = docker_params.command.split()
        command_parts.append("/app/work/config/params.yaml")
        docker_cmd.extend(command_parts)
        
        logger.info(f"Launching: {' '.join(docker_cmd)}")
        
        # Run docker command directly
        result = subprocess.run(
            docker_cmd,
            check=False,
            capture_output=True,
            text=True
        )
        
        # Log output
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.error(result.stderr)
        
        # Clean up temp file
        try:
            os.unlink(temp_path)
        except:
            pass

        if result.returncode != 0:
            return Failed(message="Docker command failed")

        return current_flow_run_id
        
    except Exception as e:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except:
            pass
        logger.error(f"Error launching docker: {str(e)}")
        return Failed(message=f"Docker command failed: {str(e)}")