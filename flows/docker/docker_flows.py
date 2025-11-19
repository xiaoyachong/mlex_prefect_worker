import tempfile
import os
import yaml
from prefect import context, flow
from prefect.states import Failed
from prefect.utilities.processutils import run_process

from flows.docker.schema import DockerParams
from flows.logger import setup_logger
from flows.credentials import add_credentials_to_io_parameters


@flow(name="Docker flow")
async def launch_docker(
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

    # Add credentials to io_parameters at the child flow level
    docker_params.params = add_credentials_to_io_parameters(docker_params.params)
    # Create temporary file for parameters
    with tempfile.NamedTemporaryFile(mode="w+t") as temp_file:
        yaml.dump(docker_params.params, temp_file)
        logger.info(f"Parameters file: {temp_file.name}")

        # Mount extra volume with parameters yaml file
        volumes = docker_params.volumes + [
            f"{temp_file.name}:/app/work/config/params.yaml"
        ]
        command = f"{docker_params.command} /app/work/config/params.yaml"

        # Define docker command
        cmd = [
            "flows/docker/bash_run_docker.sh",
            f"{docker_params.image_name}:{docker_params.image_tag}",
            command,
            " ".join(volumes),
            docker_params.network,
            " ".join(f"{k}={v}" for k, v in docker_params.env_vars.items()),
        ]
        logger.info(f"Launching with command: {cmd}")
        process = await run_process(cmd, stream_output=True)

    if process.returncode != 0:
        return Failed(message="Docker command failed")

    return current_flow_run_id