"""
Service for running Spark jobs in the chronon-spark container via Docker API.
"""

import logging
import docker
from server.config import ComputeEngine
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field

logger = logging.getLogger("uvicorn.error")



class SparkJobResponse(BaseModel):
    """Standardized response for Spark job execution."""

    status: str = Field(..., description="Status of the job: 'success' or 'error'")
    exit_code: int = Field(..., description="Exit code from the executed command")
    stdout: str = Field(..., description="Standard output from the job")
    stderr: str = Field(..., description="Standard error from the job")
    start_time: str = Field(..., description="Job start time (ISO format)")
    end_time: str = Field(..., description="Job end time (ISO format)")
    duration_seconds: float = Field(..., description="Job duration in seconds")
    command: str = Field(..., description="The actual command that was executed")
    container: Optional[str] = Field(
        None, description="Name of the container where job ran"
    )
    error: Optional[str] = Field(
        None, description="Error message when the job execution failed"
    )

class SparkJobRequest(BaseModel):
    """Request model for triggering a Spark job."""

    conf_path: str = Field(
        ...,
        description="Path to the compiled config relative to app directory",
        example="compiled/group_bys/quickstart/page_views.v1__1",
    )
    ds: str = Field(
        ..., description="Date string in YYYY-MM-DD format", example="2025-11-01"
    )
    # compute_engine: ComputeEngine = Field(..., description="Compute engine to use", example=ComputeEngine.LOCAL)
    mode: Optional[str] = Field(
        None, description="Optional mode (e.g., 'upload-to-kv')", example="upload-to-kv"
    )
    


class SparkJobRunner:
    """
    A service that executes Spark jobs in the chronon-spark container
    using the Docker SDK for Python.
    """

    def __init__(self, spark_container_name: str = "local_deployment-chronon-spark-1"):
        """
        Initialize the SparkJobRunner.

        Args:
            spark_container_name: Name or prefix of the Spark container
        """
        self.spark_container_name = spark_container_name
        self.client: Optional[docker.DockerClient] = None

    def _get_docker_client(self) -> docker.DockerClient:
        """Get or create a Docker client."""
        if self.client is None:
            try:
                self.client = docker.from_env()
                logger.info("Docker client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Docker client: {e}")
                raise RuntimeError(f"Cannot connect to Docker daemon: {e}")
        return self.client

    def _find_spark_container(self) -> docker.models.containers.Container:
        """
        Find the chronon-spark container.

        Returns:
            The Docker container object

        Raises:
            RuntimeError: If container is not found or not running
        """
        client = self._get_docker_client()

        try:
            container = client.containers.get(self.spark_container_name)
            if container.status != "running":
                raise RuntimeError(
                    f"Container '{self.spark_container_name}' exists but is not running (status: {container.status})"
                )
            logger.info(f"Found Spark container: {container.name}")
            return container
        except docker.errors.NotFound:
            raise RuntimeError(
                f"Container '{self.spark_container_name}' not found. "
                f"Please ensure the chronon-spark container is running."
            )

    def _execute(
        self,
        command: str,
        operation_name: str,
    ) -> SparkJobResponse:
        """
        Execute a command in the Spark container and return a standardized response.

        This is a centralized execution method that handles:
        - Finding the Spark container
        - Executing the command via Docker API
        - Capturing and decoding stdout/stderr
        - Timing the execution
        - Building the SparkJobResponse
        - Exception handling

        Args:
            command: The bash command to execute in the container
            operation_name: Human-readable name for logging (e.g., "database creation", "table deletion")

        Returns:
            SparkJobResponse with execution results and metadata
        """
        start_time = datetime.now()

        logger.info(f"Executing {operation_name}: {command}")

        try:
            container = self._find_spark_container()

            # Execute the command in the container
            exec_result = container.exec_run(
                cmd=["bash", "-c", command],
                stdout=True,
                stderr=True,
                stream=False,
                demux=True,  # Separate stdout and stderr
            )

            exit_code = exec_result.exit_code
            stdout_bytes, stderr_bytes = exec_result.output

            # Decode output
            stdout = stdout_bytes.decode("utf-8") if stdout_bytes else ""
            stderr = stderr_bytes.decode("utf-8") if stderr_bytes else ""

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Log results
            if exit_code == 0:
                logger.info(
                    f"{operation_name.capitalize()} completed successfully in {duration:.2f}s"
                )
            else:
                logger.error(
                    f"{operation_name.capitalize()} failed with exit code {exit_code}"
                )
                logger.error(f"stderr: {stderr}")

            return SparkJobResponse(
                status="success" if exit_code == 0 else "error",
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                command=command,
                container=container.name,
            )

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.error(f"Failed to execute {operation_name}: {e}")
            return SparkJobResponse(
                status="error",
                exit_code=-1,
                stdout="",
                stderr=str(e),
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                command=command,
                error=str(e),
            )

    def create_database(self, database_name: str) -> SparkJobResponse:
        """
        Create a database in the chronon-spark container.

        Args:
            database_name: Name of the database to create

        Returns:
            SparkJobResponse with command execution metadata
        """
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.database={database_name} "
            f"-i /srv/chronon/scripts/create-database.scala"
        )

        return self._execute(
            command=command,
            operation_name="database creation",
        )

    def delete_table(self, table_name: str, application_id: Optional[str] = None) -> SparkJobResponse:
        """
        Delete a table in the chronon-spark container.

        Args:
            table_name: Fully qualified table name (database.table_name) or just table name
            application_id: Unused for local execution (included for interface compatibility)

        Returns:
            SparkJobResponse with command execution metadata
        """
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.table={table_name} "
            f"-i /srv/chronon/scripts/delete-table.scala"
        )

        return self._execute(
            command=command,
            operation_name="table deletion",
        )

    def _upload_to_kv_command(  
        self, job_request: SparkJobRequest
    ) -> SparkJobResponse:
        """
        Run the DynamoDB bulk upload script inside the Spark container.

        Args:
            conf_path: Path to the compiled GroupBy configuration relative to /srv/chronon/app (e.g. compiled/group_bys/...).
            ds: Optional partition date (e.g., "2025-10-17") passed through to the script.

        Returns:
            SparkJobResponse with command execution metadata
        """
        jar_list = "/srv/chronon/jars/chronon-spark-assembly.jar,/srv/chronon/jars/chronon-aws-assembly.jar"
        cmd_parts = [
            "cd app && spark-shell --master local[*]",
            f"--jars {jar_list}",
            f"--conf spark.chronon.bulkput.confPath={job_request.conf_path}",
        ]
        if job_request.ds:
            cmd_parts.append(f"--conf spark.chronon.bulkput.ds={job_request.ds}")
        cmd_parts.append("-i /srv/chronon/scripts/dynamodb-bulk-put.scala")

        return " ".join(cmd_parts)


    def _regular_command(self, job_request: SparkJobRequest) -> str:

        cmd_parts = ["cd app &&", "python3 run.py"]

        # Add mode first if specified
        if job_request.mode:
            cmd_parts.append(f"--mode {job_request.mode}")

        # Add required arguments
        cmd_parts.append(f"--conf={job_request.conf_path}")
        cmd_parts.append(f"--ds {job_request.ds}")  # Use space not equals for --ds

        return " ".join(cmd_parts)

    def submit_job(
        self,
        job_request: SparkJobRequest,
    ) -> SparkJobResponse:
        """
        Execute a Spark job in the chronon-spark container.

        Args:
            job_request: SparkJobRequest containing conf_path, ds, and mode

        Returns:
            SparkJobResponse capturing stdout/stderr alongside conf_path, ds, and
            mode.
        """

        if job_request.mode == "upload-to-kv":
            command = self._upload_to_kv_command(job_request)
            operation_name = "upload-to-kv"
        else:
            command = self._regular_command(job_request)
            operation_name="Spark job"

        return self._execute(
            command=command,
            operation_name=operation_name,
        )

    def close(self):
        """Close the Docker client connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Docker client closed")
