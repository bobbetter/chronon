"""
S3 Uploader Service - Upload files to S3

This service handles uploading files from the local filesystem to an S3 bucket.
AWS credentials are read from environment variables.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from server.services.remote.boto_client import BotoClient
from server.config import S3_BUCKET

logger = logging.getLogger("uvicorn.error")

# Pydantic models for API responses (reusable in routes)
class FileInfo(BaseModel):
    """Information about a local file."""

    name: str
    path: str
    exists: bool
    size_bytes: int = 0
    size_mb: float = 0.0


class UploadResultItem(BaseModel):
    """Result of a single file upload."""

    file_name: str
    s3_key: str
    success: bool
    message: str
    size_mb: float = 0.0


class S3FileInfo(BaseModel):
    """Information about a file in S3."""

    key: str
    size_bytes: int
    size_mb: float
    last_modified: str


class ClearResult(BaseModel):
    """Result of clearing S3 files."""

    deleted: List[str] = Field(default_factory=list)
    deleted_count: int = 0
    errors: List[str] = Field(default_factory=list)
    success: bool = True


class FolderUploadResponse(BaseModel):
    """Response for folder upload endpoint."""

    bucket: str
    s3_key_prefix: str
    local_folder: str
    uploaded_files: List[UploadResultItem]
    uploaded_count: int
    failed_count: int
    total_size_mb: float


class UploadResponse(BaseModel):
    """Response for upload endpoint."""

    bucket: str
    key_prefix: str
    cleared: Optional[ClearResult] = None
    uploads: List[UploadResultItem]
    successful_count: int
    failed_count: int
    total_size_mb: float


class S3Client:
    """
    S3 uploader for files.

    Extends BotoClient to handle uploading files to S3 with support for:
    - Listing local files
    - Uploading files to S3
    - Clearing existing files from S3
    - Listing files already in S3
    """

    def __init__(
        self,
        boto_client: BotoClient,
        s3_bucket: str = S3_BUCKET,
    ) -> None:
        """
        Initialize the S3 uploader.

        Args:
            s3_bucket: S3 bucket name
            boto_client: BotoClient instance.
        """
        self.s3_bucket = s3_bucket
        self.client = boto_client.get_client("s3")

    def clear_s3_files(
        self,
        s3_key_prefix: str,
        suffix_filter: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> ClearResult:
        """
        Clear files from S3 at a given prefix.

        Supports pagination for large directories and optional suffix filtering.

        Args:
            s3_key_prefix: S3 key prefix to delete. Can be an s3:// URI
                          (e.g., 's3://bucket/path/') which will extract the bucket.
            suffix_filter: Optional. Only delete files ending with this suffix.
                          If None, deletes ALL files at the prefix.
            bucket: Optional bucket name. If not provided, uses self.s3_bucket.
                   Ignored if s3_key_prefix is an s3:// URI.

        Returns:
            ClearResult with deletion results
        """
        deleted: List[str] = []
        errors: List[str] = []

        # Parse s3:// URI if provided
        if s3_key_prefix.startswith("s3://"):
            path = s3_key_prefix[5:]
            parts = path.split("/", 1)
            bucket = parts[0]
            s3_key_prefix = parts[1] if len(parts) > 1 else ""

        target_bucket = bucket or self.s3_bucket

        logger.info(
            f"Clearing S3 objects at s3://{target_bucket}/{s3_key_prefix}"
            + (f" (suffix_filter={suffix_filter})" if suffix_filter else "")
        )

        try:
            # Use paginator to handle >1000 objects
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=target_bucket, Prefix=s3_key_prefix)

            for page in pages:
                if "Contents" not in page:
                    continue

                # Filter by suffix if specified, otherwise take all
                if suffix_filter:
                    matching_objects = [
                        {"Key": obj["Key"]}
                        for obj in page["Contents"]
                        if obj["Key"].endswith(suffix_filter)
                    ]
                else:
                    matching_objects = [{"Key": obj["Key"]} for obj in page["Contents"]]

                if matching_objects:
                    delete_response = self.client.delete_objects(
                        Bucket=target_bucket,
                        Delete={"Objects": matching_objects},
                    )

                    if "Deleted" in delete_response:
                        deleted.extend([obj["Key"] for obj in delete_response["Deleted"]])

                    if "Errors" in delete_response:
                        errors.extend([
                            f"{err['Key']}: {err['Message']}"
                            for err in delete_response["Errors"]
                        ])

            logger.info(f"Deleted {len(deleted)} objects from s3://{target_bucket}/{s3_key_prefix}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_msg = str(e)
            logger.error(f"Error deleting S3 objects: {error_msg}")
            if error_code != "NoSuchBucket":
                errors.append(error_msg)

        return ClearResult(
            deleted=deleted,
            deleted_count=len(deleted),
            errors=errors,
            success=len(errors) == 0,
        )

    def upload_file(self, file_dir: str, file_name: str, s3_key: Optional[str] = None, s3_key_prefix: Optional[str] = None) -> UploadResultItem:
        """
        Upload a single file to S3.

        Args:
            file_dir: Directory containing the file to upload
            file_name: Name of the file to upload (looked up in file_dir)
            s3_key: Optional custom S3 key. If not provided, uses s3_key_prefix + file_name

        Returns:
            UploadResultItem with operation details
        """
        file_path = Path(file_dir) / file_name
        if s3_key is None:
            if s3_key_prefix is None:
                raise ValueError("s3_key_prefix is required if s3_key is not provided")
            s3_key = f"{s3_key_prefix}{file_name}"

        if not file_path.exists():
            return UploadResultItem(
                file_name=file_name,
                s3_key=s3_key,
                success=False,
                message=f"File not found: {file_path}",
            )

        try:
            size_bytes = file_path.stat().st_size
            size_mb = size_bytes / (1024 * 1024)

            self.client.upload_file(
                str(file_path),
                self.s3_bucket,
                s3_key,
            )

            return UploadResultItem(
                file_name=file_name,
                s3_key=s3_key,
                success=True,
                message=f"Successfully uploaded to s3://{self.s3_bucket}/{s3_key}",
                size_mb=round(size_mb, 2),
            )

        except ClientError as e:
            return UploadResultItem(
                file_name=file_name,
                s3_key=s3_key,
                success=False,
                message=f"Upload failed: {e}",
            )

    def upload_all_files(self, file_dir: str, files: List[str], s3_key_prefix: str , clear_existing: bool = True) -> UploadResponse:
        """
        Upload all configured files to S3.

        Args:
            clear_existing: Whether to clear existing files before uploading

        Returns:
            UploadResponse with upload results
        """
        results: List[UploadResultItem] = []
        clear_result: Optional[ClearResult] = None

        # Optionally clear existing files
        if clear_existing:
            clear_result = self.clear_s3_files(s3_key_prefix=s3_key_prefix)

        # Upload each file
        for file_name in files:
            result = self.upload_file(file_dir, file_name, s3_key_prefix=s3_key_prefix)
            results.append(result)

        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        return UploadResponse(
            bucket=self.s3_bucket,
            key_prefix=s3_key_prefix,
            cleared=clear_result,
            uploads=results,
            successful_count=len(successful),
            failed_count=len(failed),
            total_size_mb=sum(r.size_mb for r in successful),
        )

    def upload_folder(self, local_folder: str, s3_key_prefix: str) -> FolderUploadResponse:
        """
        Upload all files from a local folder to S3 recursively.

        Args:
            local_folder: Path to the local folder to upload
            s3_key_prefix: S3 key prefix for the uploaded files (e.g., "compiled/")

        Returns:
            FolderUploadResponse with upload results
        """
        local_path = Path(local_folder)
        results: List[UploadResultItem] = []

        if not local_path.exists():
            logger.warning(f"Local folder not found: {local_path}")
            return FolderUploadResponse(
                bucket=self.s3_bucket,
                s3_key_prefix=s3_key_prefix,
                local_folder=str(local_path),
                uploaded_files=[],
                uploaded_count=0,
                failed_count=0,
                total_size_mb=0.0,
            )

        # Recursively iterate through all files in the folder
        for file_path in local_path.rglob("*"):
            if file_path.is_file():
                # Calculate relative path from the local folder
                relative_path = file_path.relative_to(local_path)
                # Build S3 key: prefix + relative path (use forward slashes)
                s3_key = f"{s3_key_prefix.rstrip('/')}/{relative_path}".replace("\\", "/")

                try:
                    size_bytes = file_path.stat().st_size
                    size_mb = size_bytes / (1024 * 1024)

                    self.client.upload_file(
                        str(file_path),
                        self.s3_bucket,
                        s3_key,
                    )

                    results.append(
                        UploadResultItem(
                            file_name=str(relative_path),
                            s3_key=s3_key,
                            success=True,
                            message=f"Successfully uploaded to s3://{self.s3_bucket}/{s3_key}",
                            size_mb=round(size_mb, 2),
                        )
                    )

                except ClientError as e:
                    results.append(
                        UploadResultItem(
                            file_name=str(relative_path),
                            s3_key=s3_key,
                            success=False,
                            message=f"Upload failed: {e}",
                        )
                    )

        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        logger.info(f"Uploaded {len(successful)} files to s3://{self.s3_bucket}/{s3_key_prefix}")
        return FolderUploadResponse(
            bucket=self.s3_bucket,
            s3_key_prefix=s3_key_prefix,
            local_folder=str(local_path),
            uploaded_files=results,
            uploaded_count=len(successful),
            failed_count=len(failed),
            total_size_mb=sum(r.size_mb for r in successful),
        )

    def ensure_bucket_exists(self) -> Dict[str, Any]:
        """
        Ensure the S3 bucket exists, creating it if necessary.

        Returns:
            Dictionary with bucket status
        """
        try:
            self.client.head_bucket(Bucket=self.s3_bucket)
            return {
                "bucket": self.s3_bucket,
                "exists": True,
                "created": False,
                "message": "Bucket already exists",
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")

            if error_code == "404" or error_code == "NoSuchBucket":
                # Bucket doesn't exist, create it
                try:
                    if self.region_name == "us-east-1":
                        self.client.create_bucket(Bucket=self.s3_bucket)
                    else:
                        self.client.create_bucket(
                            Bucket=self.s3_bucket,
                            CreateBucketConfiguration={
                                "LocationConstraint": self.region_name
                            },
                        )

                    return {
                        "bucket": self.s3_bucket,
                        "exists": True,
                        "created": True,
                        "message": f"Bucket '{self.s3_bucket}' created successfully",
                    }
                except ClientError as create_error:
                    return {
                        "bucket": self.s3_bucket,
                        "exists": False,
                        "created": False,
                        "message": f"Failed to create bucket: {create_error}",
                    }
            else:
                return {
                    "bucket": self.s3_bucket,
                    "exists": False,
                    "created": False,
                    "message": f"Failed to check bucket: {e}",
                }