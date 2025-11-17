"""
Service for running Spark jobs in the chronon-spark container via Docker API.
"""
import logging
import docker
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger("uvicorn.error")


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
            # Try to find by exact name first
            container = client.containers.get(self.spark_container_name)
            if container.status != 'running':
                raise RuntimeError(f"Container {self.spark_container_name} exists but is not running (status: {container.status})")
            return container
        except docker.errors.NotFound:
            # If not found by exact name, search for containers with the name in it
            containers = client.containers.list(filters={"status": "running"})
            for container in containers:
                if "chronon-spark" in container.name:
                    logger.info(f"Found Spark container: {container.name}")
                    return container
            
            raise RuntimeError(
                f"Chronon Spark container not found or not running. "
                f"Looking for container with name containing 'chronon-spark'"
            )
    
    def create_database(self, database_name: str) -> Dict[str, Any]:
        """
        Create a database in the chronon-spark container.
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            Dictionary with creation results including:
            - status: "success" or "error"
            - exit_code: Command exit code
            - stdout: Standard output from the command
            - stderr: Standard error from the command
            - start_time: Job start timestamp
            - end_time: Job end timestamp
            - command: The actual command that was executed
            
        Raises:
            RuntimeError: If Docker operations fail
        """
        start_time = datetime.now()
        
        # Build the spark-shell command
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.database={database_name} "
            f"-i ./scripts/create-database.scala"
        )
        
        logger.info(f"Executing database creation: {command}")
        
        try:
            container = self._find_spark_container()
            
            # Execute the command in the container
            exec_result = container.exec_run(
                cmd=["bash", "-c", command],
                stdout=True,
                stderr=True,
                stream=False,
                demux=True  # Separate stdout and stderr
            )
            
            exit_code = exec_result.exit_code
            stdout_bytes, stderr_bytes = exec_result.output
            
            # Decode output
            stdout = stdout_bytes.decode('utf-8') if stdout_bytes else ""
            stderr = stderr_bytes.decode('utf-8') if stderr_bytes else ""
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success" if exit_code == 0 else "error",
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "container": container.name,
                "database_name": database_name
            }
            
            if exit_code == 0:
                logger.info(f"Database creation completed successfully in {duration:.2f}s")
            else:
                logger.error(f"Database creation failed with exit code {exit_code}")
                logger.error(f"stderr: {stderr}")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Failed to execute database creation: {e}")
            return {
                "status": "error",
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "error": str(e),
                "database_name": database_name
            }
    
    def delete_table(self, table_name: str) -> Dict[str, Any]:
        """
        Delete a table in the chronon-spark container.
        
        Args:
            table_name: Fully qualified table name (database.table_name) or just table name
            
        Returns:
            Dictionary with deletion results including:
            - status: "success" or "error"
            - exit_code: Command exit code
            - stdout: Standard output from the command
            - stderr: Standard error from the command
            - start_time: Job start timestamp
            - end_time: Job end timestamp
            - command: The actual command that was executed
            
        Raises:
            RuntimeError: If Docker operations fail
        """
        start_time = datetime.now()
        
        # Build the spark-shell command
        command = (
            f"cd app && spark-shell --master local[*] "
            f"--conf spark.chronon.table={table_name} "
            f"-i ./scripts/delete-table.scala"
        )
        
        logger.info(f"Executing table deletion: {command}")
        
        try:
            container = self._find_spark_container()
            
            # Execute the command in the container
            exec_result = container.exec_run(
                cmd=["bash", "-c", command],
                stdout=True,
                stderr=True,
                stream=False,
                demux=True  # Separate stdout and stderr
            )
            
            exit_code = exec_result.exit_code
            stdout_bytes, stderr_bytes = exec_result.output
            
            # Decode output
            stdout = stdout_bytes.decode('utf-8') if stdout_bytes else ""
            stderr = stderr_bytes.decode('utf-8') if stderr_bytes else ""
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success" if exit_code == 0 else "error",
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "container": container.name,
                "table_name": table_name
            }
            
            if exit_code == 0:
                logger.info(f"Table deletion completed successfully in {duration:.2f}s")
            else:
                logger.error(f"Table deletion failed with exit code {exit_code}")
                logger.error(f"stderr: {stderr}")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Failed to execute table deletion: {e}")
            return {
                "status": "error",
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "error": str(e),
                "table_name": table_name
            }
        
    def run_spark_job(
        self,
        conf_path: str,
        ds: str,
        mode: Optional[str] = None,
        additional_args: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Execute a Spark job in the chronon-spark container.
        
        Args:
            conf_path: Path to the compiled config (e.g., "compiled/group_bys/quickstart/page_views.v1__1")
            ds: Date string (e.g., "2025-11-01")
            mode: Optional mode flag (e.g., "backfill", "upload", "upload-to-kv")
            additional_args: Optional dictionary of additional command-line arguments
            
        Returns:
            Dictionary with job execution results including:
            - status: "success" or "error"
            - exit_code: Command exit code
            - stdout: Standard output from the command
            - stderr: Standard error from the command
            - start_time: Job start timestamp
            - end_time: Job end timestamp
            - command: The actual command that was executed
            
        Raises:
            RuntimeError: If Docker operations fail
        """
        start_time = datetime.now()
        
        # Build the command - note that run.py uses --ds with space, not equals
        cmd_parts = [
            "cd app &&",
            "python3 run.py"
        ]
        
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
        
        logger.info(f"Executing Spark job: {command}")
        
        try:
            container = self._find_spark_container()
            
            # Execute the command in the container
            exec_result = container.exec_run(
                cmd=["bash", "-c", command],
                stdout=True,
                stderr=True,
                stream=False,
                demux=True  # Separate stdout and stderr
            )
            
            exit_code = exec_result.exit_code
            stdout_bytes, stderr_bytes = exec_result.output
            
            # Decode output
            stdout = stdout_bytes.decode('utf-8') if stdout_bytes else ""
            stderr = stderr_bytes.decode('utf-8') if stderr_bytes else ""
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success" if exit_code == 0 else "error",
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "container": container.name
            }
            
            if exit_code == 0:
                logger.info(f"Spark job completed successfully in {duration:.2f}s")
            else:
                logger.error(f"Spark job failed with exit code {exit_code}")
                logger.error(f"stderr: {stderr}")
            
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Failed to execute Spark job: {e}")
            return {
                "status": "error",
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "command": command,
                "error": str(e)
            }
    
    def close(self):
        """Close the Docker client connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Docker client closed")

