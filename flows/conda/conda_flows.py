import tempfile

import yaml
from prefect import context, flow
from prefect.states import Failed
from prefect.utilities.processutils import run_process

from flows.conda.schema import CondaParams
from flows.logger import setup_logger
from flows.credentials import add_credentials_to_io_parameters


@flow(name="launch_conda")
async def launch_conda(
    conda_params: CondaParams,
    prev_flow_run_id: str = "",
):
    logger = setup_logger()

    if (
        prev_flow_run_id != ""
        and conda_params.params["io_parameters"]["uid_retrieve"] == ""
    ):
        # Append the previous flow run id to parameters if provided
        conda_params.params["io_parameters"]["uid_retrieve"] = prev_flow_run_id

    current_flow_run_id = str(context.get_run_context().flow_run.id)

    # Append current flow run id
    conda_params.params["io_parameters"]["uid_save"] = current_flow_run_id

    # Add credentials to io_parameters at the child flow level
    conda_params.params = add_credentials_to_io_parameters(conda_params.params)

    # Create temporary file for parameters
    with tempfile.NamedTemporaryFile(mode="w+t") as temp_file:
        yaml.dump(conda_params.params, temp_file)
        # Define conda command
        cmd = [
            "flows/conda/run_conda.sh",
            conda_params.conda_env_name,
            conda_params.python_file_name,
            temp_file.name,
            conda_params.folder_name,  # Pass folder name to script
        ]
        logger.info(f"Launching with command: {cmd}")
        process = await run_process(cmd, stream_output=True)

    if process.returncode != 0:
        return Failed(message="Conda command failed")

    return current_flow_run_id


if __name__ == "__main__":
    import asyncio

    asyncio.run(launch_conda("dlsia", params={"foo": "bar"}))
