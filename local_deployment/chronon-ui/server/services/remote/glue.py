"""
AWS Glue Data Catalog Client - Manage tables in Glue Data Catalog.

This service handles:
- Table deletion from Glue Data Catalog
- Table existence checks
- Getting table S3 locations
"""

import logging
from typing import Optional, Tuple

from botocore.exceptions import ClientError

from .boto_client import BotoClient

logger = logging.getLogger("uvicorn.error")


class GlueClient:
    """
    Client for interacting with AWS Glue Data Catalog.

    Uses a BotoClient instance to create the Glue client and provides
    Glue-specific operations like deleting tables from the catalog.
    """

    def __init__(self, boto_client: BotoClient) -> None:
        """
        Initialize the Glue client.

        Args:
            boto_client: BotoClient instance for AWS credential management.
        """
        self.client = boto_client.get_client("glue")

    def _parse_table_name(self, table_name: str, database_name: Optional[str] = None) -> Tuple[str, str]:
        """
        Parse a table name into database and table components.

        Args:
            table_name: Table name. Can be fully qualified (database.table)
                    or just the table name if database_name is provided.
            database_name: The database name (optional if table_name is qualified)

        Returns:
            Tuple of (database_name, table_name)

        Raises:
            ValueError: If table name format is invalid
        """
        if "." in table_name and database_name is None:
            parts = table_name.split(".", 1)
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid table name format: '{table_name}'. "
                    "Expected 'database.table_name'"
                )
            return parts[0], parts[1]

        if not database_name:
            raise ValueError("database_name is required")

        return database_name, table_name

    def table_exists(self, database_name: str, table_name: str) -> bool:
        """
        Check if a table exists in the Glue Data Catalog.

        Args:
            database_name: The Glue database name
            table_name: The table name

        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.client.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return False
            raise

    def get_table_location(
        self,
        table_name: str,
        database_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get the S3 location of a table's data.

        Args:
            table_name: Table name (can be fully qualified as database.table)
            database_name: The Glue database name (optional if table_name is qualified)

        Returns:
            S3 URI (e.g., 's3://bucket/path/') or None if table doesn't exist
        """
        db_name, tbl_name = self._parse_table_name(table_name, database_name)

        try:
            response = self.client.get_table(DatabaseName=db_name, Name=tbl_name)
            return response["Table"]["StorageDescriptor"]["Location"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return None
            raise

    def delete_table(
        self,
        table_name: str,
        database_name: Optional[str] = None,
    ) -> dict:
        """
        Delete a table from the Glue Data Catalog.

        This only deletes the table metadata. To also delete the S3 data,
        call get_table_location() first, then use S3Client.clear_s3_files().

        Args:
            table_name: Table name. Can be fully qualified (database.table)
                       or just the table name if database_name is provided.
            database_name: The Glue database name (optional if table_name is qualified)

        Returns:
            Dict with deletion result:
                - deleted: True if table metadata was deleted
                - message: Description of what happened

        Raises:
            ValueError: If table name format is invalid
            ClientError: For other AWS API errors
        """
        db_name, tbl_name = self._parse_table_name(table_name, database_name)
        full_table_name = f"{db_name}.{tbl_name}"

        logger.info(f"Deleting table '{full_table_name}' from Glue catalog")

        # Check if table exists first
        if not self.table_exists(db_name, tbl_name):
            logger.info(f"Table '{full_table_name}' does not exist. Nothing to delete.")
            return {
                "deleted": False,
                "message": f"Table '{full_table_name}' does not exist",
            }

        # Delete the table metadata
        try:
            self.client.delete_table(DatabaseName=db_name, Name=tbl_name)
            logger.info(f"✅ Successfully deleted table '{full_table_name}'")
            return {
                "deleted": True,
                "message": f"Successfully deleted table '{full_table_name}'",
            }
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_msg = e.response["Error"]["Message"]
            logger.error(f"Failed to delete table: {error_code} - {error_msg}")
            raise

