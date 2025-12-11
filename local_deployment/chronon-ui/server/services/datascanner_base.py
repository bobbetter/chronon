"""
Abstract base class for data scanning operations.

This module defines the interface that all data scanner implementations must follow,
whether scanning local Spark warehouse data or remote AWS Glue tables.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import numpy as np


class DataScannerBase(ABC):
    """
    Abstract base class for scanning and querying data sources.

    Implementations:
    - DataScannerLocal: Scans local Spark warehouse using DuckDB
    - DataScannerRemote: Scans AWS Glue catalog using boto3
    """

    @abstractmethod
    def list_databases(self) -> List[str]:
        """
        List all databases in the data source.

        Returns:
            List of database names
        """
        pass

    @abstractmethod
    def list_tables(self, db_name: str, with_db_name: bool = False) -> List[str]:
        """
        List all tables in a given database.

        Args:
            db_name: Name of the database
            with_db_name: If True, prefix table names with database name

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def get_table_schema(self, db_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get the schema of a table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            List of column definitions with 'name' and 'type' keys
        """
        pass

    @abstractmethod
    def get_table_exists(self, db_name: str, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            True if the table exists, False otherwise
        """
        pass

    @abstractmethod
    def sample_table(
        self, db_name: str, table_name: str, limit: int = 100, offset: int = 0
    ) -> Dict[str, Any]:
        """
        Get a sample of rows from a table.

        Args:
            db_name: Name of the database
            table_name: Name of the table
            limit: Maximum number of rows to return
            offset: Number of rows to skip

        Returns:
            Dictionary containing:
                - data: List of dictionaries representing rows
                - table_schema: List of column definitions
                - row_count: Total number of rows in the table
                - limit: The limit used
                - offset: The offset used
        """
        pass

    @abstractmethod
    def get_table_stats(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about a table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            Dictionary containing table statistics including:
                - row_count: Number of rows
                - column_count: Number of columns
                - columns: List of column definitions
        """
        pass

    def execute_query(self, query: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a custom SQL query against the data source.

        Note: This method is optional and may not be supported by all implementations.
        Remote implementations may use AWS Athena for query execution.

        Args:
            query: SQL query to execute
            limit: Optional limit on number of rows returned

        Returns:
            Dictionary containing query results and schema

        Raises:
            NotImplementedError: If the implementation does not support custom queries
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support execute_query"
        )

    @staticmethod
    def _convert_numpy_to_native(obj: Any) -> Any:
        """
        Convert numpy types to native Python types for JSON serialization.

        Args:
            obj: Object to convert

        Returns:
            Object with numpy types converted to native Python types
        """
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, bytearray):
            return str(obj)
        elif isinstance(obj, dict):
            return {
                key: DataScannerBase._convert_numpy_to_native(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, (list, tuple)):
            return [DataScannerBase._convert_numpy_to_native(item) for item in obj]
        else:
            return obj

