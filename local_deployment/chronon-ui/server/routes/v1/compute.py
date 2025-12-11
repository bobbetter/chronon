"""
API routes for triggering Chronon compute operations (backfill, upload, etc.)
"""

import os
from typing import Optional, Union
from fastapi import APIRouter, HTTPException, Depends, Request, Query
from pydantic import BaseModel, Field
from server.services.runner import SparkJobRunner, SparkJobResponse, SparkJobRequest
from server.config import ComputeEngine
import logging
from server.services.remote.emr import EmrServerlessRunner, EmrJobRequest, EmrJobResult
from server.services.remote.glue import GlueClient
from server.services.remote.s3 import S3Client

logger = logging.getLogger("uvicorn.error")

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/compute/spark", tags=["compute"])

spark_runner = SparkJobRunner()

def get_spark_runner(
    request: Request, 
    compute_engine: ComputeEngine=Query(
        default=ComputeEngine.LOCAL,
        description="Compute engine to use: 'local' for local Spark Job Runner, 'remote' for EMR Serverless",
    )
):
    if compute_engine == ComputeEngine.REMOTE:
        logger.info("Using EMR Serverless Spark Job Runner")
        return EmrServerlessRunner(boto_client=request.app.state.boto_client)
    else:
        logger.info("Using local Spark Job Runner")
        return request.app.state.spark_runner


@router.post("/run-job", 
    # Todo: Unify submit_job method in SparkJobRunner and EmrServerlessRunner
    # so that the response model can be the same for both.
    response_model=Union[SparkJobResponse, EmrJobResult])
async def run_spark_job(
    job_request: Union[EmrJobRequest, SparkJobRequest], 
    spark_runner = Depends(get_spark_runner)
):
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
    
    logger.info(
        f"Received Spark job request: {job_request.model_dump_json()}"
    )

    if job_request.mode == "backfill":
        job_request.mode  == None
    elif job_request.mode  == "pre-compute-upload":
        job_request.mode  = "upload"
    elif job_request.mode  == "upload-to-kv":
        job_request.mode  = "upload-to-kv"
    else:
        raise ValueError(f"Invalid mode: {job_request.mode}")

    try:
        return spark_runner.submit_job(job_request)
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


class DeleteTableRequest(BaseModel):
    """Request model for deleting a table."""
    table_name: str = Field(..., description="Fully qualified table name (database.table_name)")


class GlueDeleteTableResponse(BaseModel):
    """Response model for Glue delete table operation."""
    deleted: bool = Field(..., description="Whether the table was deleted")
    message: str = Field(..., description="Description of what happened")


@router.post("/delete-table", response_model=Union[SparkJobResponse, GlueDeleteTableResponse])
async def delete_table(
    request: Request,
    body: DeleteTableRequest,
    compute_engine: ComputeEngine = Query(
        default=ComputeEngine.LOCAL,
        description="Compute engine: 'local' for Docker/Spark, 'remote' for AWS Glue API",
    ),
):
    """
    Delete a table from the data catalog.

    Supports both local (Docker container with Spark) and remote (AWS Glue API) execution.
    
    - **Local**: Runs spark-submit in the chronon-spark container to drop the table
    - **Remote**: Uses AWS Glue Data Catalog API directly (faster, no EMR needed)

    Args:
        body: DeleteTableRequest containing table_name
        compute_engine: 'local' or 'remote'

    Returns:
        SparkJobResponse (local) or GlueDeleteTableResponse (remote)

    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(f"Received delete-table request: table={body.table_name}, engine={compute_engine}")

        if compute_engine == ComputeEngine.REMOTE:
            glue_client = GlueClient(boto_client=request.app.state.boto_client)

            s3_location = glue_client.get_table_location(body.table_name)
            logger.info(f"Deleting S3 data at: {s3_location}")
            s3_client = S3Client(boto_client=request.app.state.boto_client)
            clear_result = s3_client.clear_s3_files(s3_location)
            if clear_result.errors:
                logger.warning(f"Some S3 deletions failed: {clear_result.errors}")

            result = glue_client.delete_table(table_name=body.table_name)

            return GlueDeleteTableResponse(**result)
        else:
            # Use local Spark container (always deletes data for managed tables)
            spark_runner: SparkJobRunner = request.app.state.spark_runner
            return spark_runner.delete_table(table_name=body.table_name)

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

