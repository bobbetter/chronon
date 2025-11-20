"""
API routes for triggering Chronon actions (backfill, upload, etc.)
"""

import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from server.services.runner import SparkJobRunner, SparkJobResponse
import logging

logger = logging.getLogger("uvicorn.error")

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/actions", tags=["actions"])


# Initialize the SparkJobRunner
spark_runner = SparkJobRunner()


# Request/Response models
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
    mode: Optional[str] = Field(
        None, description="Optional mode (e.g., 'upload-to-kv')", example="upload-to-kv"
    )


@router.post("/run-spark-job", response_model=SparkJobResponse)
async def run_spark_job(request: SparkJobRequest):
    """
    Execute a Spark job in the chronon-spark container.

    This endpoint triggers a Spark job by executing commands in the chronon-spark
    container via the Docker API. It's equivalent to running:

    ```
    docker exec chronon-spark bash -c "cd app && python3 run.py --conf=<conf_path> --ds=<ds>"
    ```

    Args:
        request: The Spark job request containing config path, date, and optional parameters

    Returns:
        SparkJobResponse with job execution results including stdout, stderr, and exit code

    Raises:
        HTTPException: If the job execution fails
    """
    try:
        logger.info(
            f"Received Spark job request: conf={request.conf_path}, ds={request.ds}, mode={request.mode}"
        )

        if request.mode == "backfill":
            request.mode = None

        if request.mode == "pre-compute-upload":
            request.mode = "upload"

        if request.mode == "upload-to-kv":
            result = spark_runner.upload_to_kv(
                conf_path=request.conf_path, ds=request.ds
            )
        else:
            result = spark_runner.run_spark_job(
                conf_path=request.conf_path,
                ds=request.ds,
                mode=request.mode,
            )

        return result

    except Exception as e:
        logger.error(f"Unexpected error running Spark job: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to execute Spark job: {str(e)}"
        )


@router.post("/create-database", response_model=SparkJobResponse)
async def create_database(database_name: str):
    """
    Create a database in the chronon-spark container.

    Args:
        database_name: Name of the database to create

    Returns:
        SparkJobResponse with creation results including stdout, stderr, and exit code

    Raises:
        HTTPException: If the database creation fails
    """
    try:
        logger.info(f"Received database creation request: database={database_name}")
        return spark_runner.create_database(database_name)
    except Exception as e:
        logger.error(f"Unexpected error creating database: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create database: {str(e)}"
        )


@router.post("/delete-table", response_model=SparkJobResponse)
async def delete_table(table_name: str):
    """
    Delete a table in the chronon-spark container.
    """
    try:
        return spark_runner.delete_table(table_name)
    except Exception as e:
        logger.error(f"Unexpected error deleting table: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete table: {str(e)}")


@router.get("/health")
async def health_check():
    """
    Health check endpoint to verify Docker connectivity.

    Returns:
        Status indicating whether the service can connect to Docker and find the Spark container
    """
    try:
        container = spark_runner._find_spark_container()
        return {
            "status": "healthy",
            "docker_connected": True,
            "spark_container": container.name,
            "spark_container_status": container.status,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "docker_connected": False, "error": str(e)}
