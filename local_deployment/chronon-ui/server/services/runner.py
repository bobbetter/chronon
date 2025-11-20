"""
Service for running Spark jobs in the chronon-spark container via Docker API.
"""

import logging
import docker
from typing import Optional, Dict, Any
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
    extra_parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional parameters specific to a given operation",
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
        extra_parameters: Optional[Dict[str, Any]] = None,
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
            extra_parameters: Optional dict of parameters to include in the response

        Returns:
            SparkJobResponse with execution results and metadata
        """
        start_time = datetime.now()
        extra_params = extra_parameters or {}

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
                extra_parameters=extra_params,
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
                extra_parameters=extra_params,
            )

    def create_database(self, database_name: str) -> SparkJobResponse:
        """
        Create a database in the chronon-spark container.

        Args:
            database_name: Name of the database to create

        Returns:
            SparkJobResponse with command execution metadata and the database name
            placed in the extra_parameters field.
        """
        # Build the spark-shell command
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.database={database_name} "
            f"-i ./scripts/create-database.scala"
        )

        return self._execute(
            command=command,
            operation_name="database creation",
            extra_parameters={"database_name": database_name},
        )

    def delete_table(self, table_name: str) -> SparkJobResponse:
        """
        Delete a table in the chronon-spark container.

        Args:
            table_name: Fully qualified table name (database.table_name) or just table name

        Returns:
            SparkJobResponse with command execution metadata and the table name
            placed in the extra_parameters field.
        """
        # Build the spark-shell command
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.table={table_name} "
            f"-i ./scripts/delete-table.scala"
        )

        return self._execute(
            command=command,
            operation_name="table deletion",
            extra_parameters={"table_name": table_name},
        )

    def upload_to_kv(
        self, conf_path: str, ds: Optional[str] = None
    ) -> SparkJobResponse:
        """
        Run the DynamoDB bulk upload script inside the Spark container.

        Args:
            conf_path: Path to the compiled GroupBy configuration relative to /srv/chronon/app (e.g. compiled/group_bys/...).
            ds: Optional partition date (e.g., "2025-10-17") passed through to the script.

        Returns:
            SparkJobResponse mirroring other operations with conf_path/ds stored
            in the extra_parameters field.
        """
        jar_list = "/srv/chronon/jars/chronon-spark-assembly.jar,/srv/chronon/jars/chronon-aws-assembly.jar"
        cmd_parts = [
            "cd app && spark-shell --master local[*]",
            f"--jars {jar_list}",
            f"--conf spark.chronon.bulkput.confPath={conf_path}",
        ]
        if ds:
            cmd_parts.append(f"--conf spark.chronon.bulkput.ds={ds}")
        cmd_parts.append("-i ./scripts/dynamodb-bulk-put.scala")

        command = " ".join(cmd_parts)

        extra_parameters = {"conf_path": conf_path}
        if ds:
            extra_parameters["ds"] = ds

        return self._execute(
            command=command,
            operation_name="upload-to-kv",
            extra_parameters=extra_parameters,
        )

    def run_spark_job(
        self,
        conf_path: str,
        ds: str,
        mode: Optional[str] = None,
        additional_args: Optional[Dict[str, str]] = None,
    ) -> SparkJobResponse:
        """
        Execute a Spark job in the chronon-spark container.

        Args:
            conf_path: Path to the compiled config (e.g., "compiled/group_bys/quickstart/page_views.v1__1")
            ds: Date string (e.g., "2025-11-01")
            mode: Optional mode flag (e.g., "backfill", "upload", "upload-to-kv")
            additional_args: Optional dictionary of additional command-line arguments

        Returns:
            SparkJobResponse capturing stdout/stderr alongside conf_path, ds, and
            optional args stored in the extra_parameters field.
        """
        # Build the command - note that run.py uses --ds with space, not equals
        cmd_parts = ["cd app &&", "python3 run.py"]

        # Add mode first if specified
        if mode:
            cmd_parts.append(f"--mode {mode}")

        # Add required arguments
        cmd_parts.append(f"--conf={conf_path}")
        cmd_parts.append(f"--ds {ds}")  # Use space not equals for --ds

        # Add any additional arguments
        if additional_args:
            for key, value in additional_args.items():
                if value:
                    cmd_parts.append(f"--{key}={value}")
                else:
                    cmd_parts.append(f"--{key}")

        command = " ".join(cmd_parts)
        extra_parameters: Dict[str, Any] = {"conf_path": conf_path, "ds": ds}
        if mode:
            extra_parameters["mode"] = mode
        if additional_args:
            extra_parameters["additional_args"] = additional_args

        return self._execute(
            command=command,
            operation_name="Spark job",
            extra_parameters=extra_parameters,
        )

    def close(self):
        """Close the Docker client connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Docker client closed")
