"""
Remote data scanner implementation using AWS Glue Data Catalog.

This module provides the DataScannerRemote class for scanning and querying
AWS Glue tables.
"""

import logging
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from ..datascanner_base import DataScannerBase
from .boto_client import BotoClient

logger = logging.getLogger("uvicorn.error")


# Mapping from Glue/Hive types to simplified type names
GLUE_TYPE_MAPPING = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "tinyint": "TINYINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "binary": "BLOB",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "decimal": "DECIMAL",
}


class DataScannerRemote(DataScannerBase):
    """
    A class to scan and query AWS Glue Data Catalog tables.

    This implementation uses the AWS Glue API to list databases, tables, and schemas.
    """

    def __init__(self, boto_client: Optional[BotoClient] = None):
        """
        Initialize the DataScannerRemote.

        Args:
            boto_client: BotoClient instance for AWS API calls. If None, creates a new one.
        """
        self._boto_client = boto_client or BotoClient()
        self._glue_client = None

    @property
    def glue_client(self):
        """Lazily create and cache the Glue client."""
        if self._glue_client is None:
            self._glue_client = self._boto_client.get_client("glue")
        return self._glue_client

    def list_databases(self) -> List[str]:
        """
        List all databases in the Glue Data Catalog.

        Returns:
            List of database names
        """
        databases = []
        paginator = self.glue_client.get_paginator("get_databases")

        try:
            for page in paginator.paginate():
                for db in page.get("DatabaseList", []):
                    databases.append(db["Name"])
        except ClientError as e:
            logger.error(f"Error listing Glue databases: {e}")
            raise

        return sorted(databases)

    def list_tables(self, db_name: str, with_db_name: bool = False) -> List[str]:
        """
        List all tables in a given Glue database.

        Args:
            db_name: Name of the database
            with_db_name: If True, prefix table names with database name

        Returns:
            List of table names
        """
        tables = []
        paginator = self.glue_client.get_paginator("get_tables")

        try:
            for page in paginator.paginate(DatabaseName=db_name):
                for table in page.get("TableList", []):
                    table_name = table["Name"]
                    if with_db_name:
                        tables.append(f"{db_name}.{table_name}")
                    else:
                        tables.append(table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.warning(f"Database '{db_name}' not found in Glue catalog")
                return []
            logger.error(f"Error listing tables in database '{db_name}': {e}")
            raise

        return sorted(tables)

    def get_table_schema(self, db_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get the schema of a Glue table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            List of column definitions with name and type
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=db_name, Name=table_name
            )
            table_info = response.get("Table", {})
            storage_descriptor = table_info.get("StorageDescriptor", {})
            columns = storage_descriptor.get("Columns", [])

            # Also include partition keys in the schema
            partition_keys = table_info.get("PartitionKeys", [])

            schema = []
            for col in columns:
                col_type = col.get("Type", "string")
                # Normalize type names
                normalized_type = GLUE_TYPE_MAPPING.get(
                    col_type.lower().split("(")[0], col_type.upper()
                )
                schema.append({"name": col["Name"], "type": normalized_type})

            for pk in partition_keys:
                pk_type = pk.get("Type", "string")
                normalized_type = GLUE_TYPE_MAPPING.get(
                    pk_type.lower().split("(")[0], pk_type.upper()
                )
                schema.append({"name": pk["Name"], "type": normalized_type})

            return schema

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                logger.warning(
                    f"Table '{db_name}.{table_name}' not found in Glue catalog"
                )
                return []
            logger.error(f"Error getting schema for '{db_name}.{table_name}': {e}")
            raise

    def get_table_exists(self, db_name: str, table_name: str) -> bool:
        """
        Check if a table exists in the Glue Data Catalog.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.glue_client.get_table(DatabaseName=db_name, Name=table_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return False
            logger.error(
                f"Error checking existence of '{db_name}.{table_name}': {e}"
            )
            raise

    def sample_table(
        self, db_name: str, table_name: str, limit: int = 100, offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get a sample of rows from a Glue table.

        Note: This method is not yet implemented for remote scanning.

        Raises:
            NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError(
            "sample_table is not yet implemented for DataScannerRemote. "
            "Athena integration is required for data sampling."
        )

    def get_table_stats(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about a Glue table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            Dictionary containing table statistics
        """
        schema = self.get_table_schema(db_name, table_name)

        # Get table metadata for additional info
        try:
            response = self.glue_client.get_table(
                DatabaseName=db_name, Name=table_name
            )
            table_info = response.get("Table", {})
            parameters = table_info.get("Parameters", {})

            # Try to get row count from table parameters (if available)
            row_count = None
            if "recordCount" in parameters:
                try:
                    row_count = int(parameters["recordCount"])
                except (ValueError, TypeError):
                    pass
            elif "numRows" in parameters:
                try:
                    row_count = int(parameters["numRows"])
                except (ValueError, TypeError):
                    pass

            return {
                "row_count": row_count,
                "column_count": len(schema),
                "columns": schema,
                "location": table_info.get("StorageDescriptor", {}).get("Location"),
                "table_type": table_info.get("TableType"),
                "create_time": str(table_info.get("CreateTime", "")),
                "last_access_time": str(table_info.get("LastAccessTime", "")),
            }

        except ClientError as e:
            logger.error(f"Error getting stats for '{db_name}.{table_name}': {e}")
            raise

    def get_table_location(self, db_name: str, table_name: str) -> Optional[str]:
        """
        Get the S3 location of a Glue table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            S3 location string or None if not found
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=db_name, Name=table_name
            )
            return response.get("Table", {}).get("StorageDescriptor", {}).get("Location")
        except ClientError:
            return None

    def get_table_partitions(
        self, db_name: str, table_name: str, max_partitions: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get partition information for a Glue table.

        Args:
            db_name: Name of the database
            table_name: Name of the table
            max_partitions: Maximum number of partitions to return

        Returns:
            List of partition information dictionaries
        """
        partitions = []
        try:
            paginator = self.glue_client.get_paginator("get_partitions")
            for page in paginator.paginate(
                DatabaseName=db_name,
                TableName=table_name,
                PaginationConfig={"MaxItems": max_partitions},
            ):
                for partition in page.get("Partitions", []):
                    partitions.append(
                        {
                            "values": partition.get("Values", []),
                            "location": partition.get("StorageDescriptor", {}).get(
                                "Location"
                            ),
                            "creation_time": str(partition.get("CreationTime", "")),
                        }
                    )
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return []
            logger.error(
                f"Error getting partitions for '{db_name}.{table_name}': {e}"
            )
            raise

        return partitions
