"""
API routes for uploading Chronon files to S3.
"""
import os
import logging
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel, Field

from server.services.remote.s3 import (
    S3Client,
    FolderUploadResponse,
    UploadResponse,
)
from server import config as cfg

logger = logging.getLogger("uvicorn.error")
router = APIRouter(prefix="/v1/remote-resources/upload_s3", tags=["remote-resources"])


def get_s3_client(request: Request) -> S3Client:
    """Get S3 client using the shared boto_client from app.state."""
    return S3Client(boto_client=request.app.state.boto_client)

class UploadRequest(BaseModel):
    """Request for upload endpoint."""

    clear_existing: bool = Field(
        default=True,
        description="Whether to clear existing files from S3 before uploading",
    )



@router.post("/jars", response_model=UploadResponse)
async def upload_jars(request: UploadRequest = UploadRequest(), s3_client: S3Client = Depends(get_s3_client)):
    """
    Upload Chronon JAR files to S3.

    This endpoint uploads the configured JAR files (chronon-spark-assembly.jar,
    chronon-aws-assembly.jar) to the configured S3 bucket.

    Args:
        request: Upload options (clear_existing defaults to True)

    Returns:
        UploadResponse with details about uploaded files
    """
    try:
        logger.info(
            f"Starting file upload to S3 (clear_existing={request.clear_existing})"
        )

        # Ensure bucket exists before uploading
        bucket_status = s3_client.ensure_bucket_exists()
        if not bucket_status["exists"]:
            raise HTTPException(
                status_code=500,
                detail=f"S3 bucket not available: {bucket_status['message']}",
            )

        # Upload all files
        result = s3_client.upload_all_files(
            file_dir=cfg.CHRONON_JAR_DIR,
            files=cfg.CHRONON_JAR_FILES,
            s3_key_prefix=cfg.CHRONON_JAR_S3_KEY_PREFIX,
            clear_existing=request.clear_existing,
        )

        logger.info(
            f"File upload complete: {result.successful_count} succeeded, "
            f"{result.failed_count} failed"
        )

        return result

    except Exception as e:
        logger.error(f"Error uploading files: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to upload files: {str(e)}"
        )


@router.get("/debug/list-bucket")
async def debug_list_bucket(prefix: str = cfg.CHRONON_JAR_S3_KEY_PREFIX, s3_client: S3Client = Depends(get_s3_client)):
    """
    DEBUG: List objects in the S3 bucket.

    Args:
        prefix: Optional prefix to filter objects

    Returns:
        Raw S3 list_objects_v2 response
    """
    try:
        logger.info(f"DEBUG: Listing bucket {s3_client.s3_bucket} with prefix '{prefix}'")

        response = s3_client.client.list_objects_v2(
            Bucket=s3_client.s3_bucket,
            Prefix=prefix,
        )

        # Convert to JSON-serializable format
        contents = []
        if "Contents" in response:
            for obj in response["Contents"]:
                contents.append({
                    "Key": obj["Key"],
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"].isoformat(),
                })

        return {
            "bucket": s3_client.s3_bucket,
            "prefix": prefix or s3_client.s3_key_prefix,
            "count": len(contents),
            "contents": contents,
        }

    except Exception as e:
        logger.error(f"DEBUG: Error listing bucket: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to list bucket: {str(e)}"
        )


@router.post("/emr_scripts")
async def upload_emr_scripts(s3_client: S3Client = Depends(get_s3_client)):
    """
    Uploads the EMR scripts to S3.

    Returns:
        Upload result with S3 location
    """
    try:
        logger.info(
            f"Uploading EMR scripts to s3://{s3_client.s3_bucket}/{cfg.EMR_SCRIPTS_S3_KEY_PREFIX}"
        )

        result = s3_client.upload_folder(
            local_folder=str(cfg.EMR_SCRIPTS_DIR),
            s3_key_prefix=cfg.EMR_SCRIPTS_S3_KEY_PREFIX,
        )
        return result
    except Exception as e:
        logger.error(f"Error uploading EMR scripts: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to upload EMR scripts: {str(e)}"
        )

@router.post("/compiled_configs", response_model=FolderUploadResponse)
async def upload_compiled_configs(s3_client: S3Client = Depends(get_s3_client)):
    """
    Upload the compiled configs to S3.

    Uploads the compiled configs to s3://<bucket>/compiled/

    Returns:
        Upload result with S3 location
    """
    try:
        logger.info(
            f"Uploading compiled configs to s3://{s3_client.s3_bucket}/{cfg.COMPILED_CONFIGS_S3_KEY_PREFIX}/"
        )

        result = s3_client.upload_folder(
            local_folder=str(cfg.chronon_compiled_dir),
            s3_key_prefix=cfg.COMPILED_CONFIGS_S3_KEY_PREFIX,
        )
        return result
    except Exception as e:
        logger.error(f"Error uploading compiled configs: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to upload compiled configs: {str(e)}"
        )