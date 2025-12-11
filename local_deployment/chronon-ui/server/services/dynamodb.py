"""
DynamoDB client for checking online table existence.

This module provides a DynamoDB client that works with both local (docker-compose)
and remote (AWS) DynamoDB instances. The same class is used for both - only the
endpoint URL and credentials differ.

Usage:
    # Local DynamoDB (docker-compose)
    local_client = DynamoDBClient(
        endpoint_url="http://dynamodb-local:8000",
        region_name="us-west-2",
        access_key_id="local",
        secret_access_key="local",
    )

    # Remote DynamoDB (AWS)
    remote_client = DynamoDBClient(boto_client=boto_client)

    # Check if table exists
    exists = client.table_exists("MY_TABLE_BATCH")
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from .remote.boto_client import BotoClient

logger = logging.getLogger("uvicorn.error")

# Default configuration for local DynamoDB (docker-compose)
LOCAL_ENDPOINT_URL = os.environ.get("DYNAMO_ENDPOINT_LOCAL", "http://dynamodb-local:8000")
LOCAL_REGION = os.environ.get("AWS_DEFAULT_REGION_LOCAL", "us-west-2")
LOCAL_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID_LOCAL", "local")
LOCAL_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY_LOCAL", "local")


def _sanitize_table_name(name: str) -> str:
    """
    Sanitize a dataset name to match the DynamoDB table naming convention.
    
    This mirrors the Scala `sanitize` method used in Chronon:
    - Replace dots with underscores
    - Convert to uppercase
    - Append _BATCH suffix if not already present
    
    Args:
        name: The dataset/config name (e.g., "logins.v1__1")
        
    Returns:
        Sanitized table name (e.g., "LOGINS_V1__1_BATCH")
    """
    # Replace dots and other special chars with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    sanitized = sanitized.upper()
    
    # Add _BATCH suffix if not already present
    if not sanitized.endswith("_BATCH") and not sanitized.endswith("_STREAMING"):
        sanitized = sanitized + "_BATCH"
    
    return sanitized


class DynamoDBClient:
    """
    Client for interacting with DynamoDB to check online table existence.
    
    This client can be configured to work with either:
    - Local DynamoDB (via explicit endpoint URL and credentials)
    - Remote AWS DynamoDB (via BotoClient with SSO/IAM credentials)
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        region_name: str = LOCAL_REGION,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        boto_client: Optional[BotoClient] = None,
    ) -> None:
        """
        Initialize the DynamoDB client.
        
        For local DynamoDB:
            client = DynamoDBClient(
                endpoint_url="http://dynamodb-local:8000",
                access_key_id="local",
                secret_access_key="local",
            )
        
        For remote AWS DynamoDB:
            client = DynamoDBClient(boto_client=my_boto_client)
        
        Args:
            endpoint_url: DynamoDB endpoint URL (for local/custom endpoints)
            region_name: AWS region name
            access_key_id: AWS access key ID (for local/explicit credentials)
            secret_access_key: AWS secret access key (for local/explicit credentials)
            boto_client: BotoClient instance for AWS credentials (for remote)
        """
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._boto_client = boto_client
        self._client = None

    @property
    def client(self):
        """Lazily create and cache the DynamoDB client."""
        if self._client is not None:
            return self._client

        if self._boto_client:
            # Use BotoClient for remote AWS DynamoDB
            logger.debug("Creating DynamoDB client via BotoClient (remote)")
            self._client = self._boto_client.get_client("dynamodb")
        else:
            # Create client with explicit credentials (for local)
            logger.debug(f"Creating DynamoDB client with endpoint: {self._endpoint_url}")
            client_kwargs: Dict[str, Any] = {
                "region_name": self._region_name,
            }
            if self._endpoint_url:
                client_kwargs["endpoint_url"] = self._endpoint_url
            if self._access_key_id and self._secret_access_key:
                client_kwargs["aws_access_key_id"] = self._access_key_id
                client_kwargs["aws_secret_access_key"] = self._secret_access_key
            
            self._client = boto3.client("dynamodb", **client_kwargs)

        return self._client

    def table_exists(self, table_name: str, sanitize: bool = True) -> bool:
        """
        Check if a DynamoDB table exists.
        
        Args:
            table_name: The table name or dataset name to check
            sanitize: If True, sanitize the name to match Chronon's naming convention
                     (uppercase, replace dots with underscores, add _BATCH suffix)
        
        Returns:
            True if the table exists, False otherwise
        """
        if sanitize:
            table_name = _sanitize_table_name(table_name)
        
        try:
            self.client.describe_table(TableName=table_name)
            logger.info(f"DynamoDB table '{table_name}' exists")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                logger.info(f"DynamoDB table '{table_name}' does not exist")
                return False
            # Log and re-raise unexpected errors
            logger.error(f"Error checking DynamoDB table '{table_name}': {e}")
            raise

    def list_tables(self, limit: Optional[int] = None) -> List[str]:
        """
        List all DynamoDB tables.
        
        Args:
            limit: Maximum number of tables to return (None for all)
            
        Returns:
            List of table names
        """
        tables: List[str] = []
        last_evaluated_table_name: Optional[str] = None

        while True:
            kwargs: Dict[str, Any] = {}
            if last_evaluated_table_name:
                kwargs["ExclusiveStartTableName"] = last_evaluated_table_name
            if limit:
                kwargs["Limit"] = min(limit - len(tables), 100)

            response = self.client.list_tables(**kwargs)
            tables.extend(response.get("TableNames", []))

            # Check if we've reached the desired limit
            if limit and len(tables) >= limit:
                tables = tables[:limit]
                break

            # Check for more tables
            last_evaluated_table_name = response.get("LastEvaluatedTableName")
            if not last_evaluated_table_name:
                break

        return tables

    def get_table_info(self, table_name: str, sanitize: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get information about a DynamoDB table.
        
        Args:
            table_name: The table name or dataset name
            sanitize: If True, sanitize the name to match Chronon's naming convention
            
        Returns:
            Dictionary with table information, or None if table doesn't exist
        """
        if sanitize:
            table_name = _sanitize_table_name(table_name)

        try:
            response = self.client.describe_table(TableName=table_name)
            table_desc = response.get("Table", {})
            return {
                "table_name": table_desc.get("TableName"),
                "table_status": table_desc.get("TableStatus"),
                "item_count": table_desc.get("ItemCount"),
                "table_size_bytes": table_desc.get("TableSizeBytes"),
                "creation_date_time": str(table_desc.get("CreationDateTime", "")),
            }
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceNotFoundException":
                return None
            raise


def create_local_dynamodb_client() -> DynamoDBClient:
    """
    Factory function to create a DynamoDB client for local (docker-compose) DynamoDB.
    
    Returns:
        DynamoDBClient configured for local DynamoDB
    """
    return DynamoDBClient(
        endpoint_url=LOCAL_ENDPOINT_URL,
        region_name=LOCAL_REGION,
        access_key_id=LOCAL_ACCESS_KEY,
        secret_access_key=LOCAL_SECRET_KEY,
    )


def create_remote_dynamodb_client(boto_client: BotoClient) -> DynamoDBClient:
    """
    Factory function to create a DynamoDB client for remote AWS DynamoDB.
    
    Args:
        boto_client: BotoClient with AWS credentials
        
    Returns:
        DynamoDBClient configured for remote AWS DynamoDB
    """
    return DynamoDBClient(boto_client=boto_client)

