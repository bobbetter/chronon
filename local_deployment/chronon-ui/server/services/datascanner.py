from pathlib import Path
from typing import List, Dict, Any, Optional
import duckdb
import pandas as pd
import numpy as np
from pydantic_core.core_schema import none_schema


class DataScanner:
    """
    A class to scan and query Spark warehouse data using DuckDB.
    Similar to duckui/app.py but as a service class.
    """

    def __init__(self, warehouse_path: str):
        """
        Initialize the DataScanner with a warehouse path.
        
        Args:
            warehouse_path: Path to the Spark warehouse directory
        """
        self.warehouse_path = Path(warehouse_path)

    def list_databases(self) -> List[str]:
        """
        List all databases in the warehouse.
        Databases are directories ending with .db plus the implicit 'default' database.
        
        Returns:
            List of database names
        """
        if not self.warehouse_path.exists():
            return []
        
        dbs = [p.stem for p in self.warehouse_path.iterdir() 
               if p.is_dir() and p.suffix == ".db"]
        
        # Detect presence of top-level tables which belong to the implicit 'default' db
        default_candidates = []
        for p in self.warehouse_path.iterdir():
            if not p.is_dir():
                continue
            if p.suffix == ".db":
                continue
            if p.name in {"metastore_db"}:
                continue
            if self._has_parquet_under(p):
                default_candidates.append(p.name)
        
        if default_candidates and "default" not in dbs:
            dbs.append("default")
        
        return sorted(set(dbs))

    def list_tables(self, db_name: str, with_db_name: bool=False) -> List[str]:
        """
        List all tables in a given database.
        
        Args:
            db_name: Name of the database
            
        Returns:
            List of table names
        """
        if db_name == "default":
            tables = []
            for p in self.warehouse_path.iterdir():
                if not p.is_dir():
                    continue
                if p.suffix == ".db":
                    continue
                if p.name in {"metastore_db"}:
                    continue
                if self._has_parquet_under(p):
                    tables.append(p.name)
            return sorted(tables)
        
        db_dir = self.warehouse_path / f"{db_name}.db"
        if not db_dir.exists():
            return []
        if with_db_name:
            return [f"{db_name}.{p.name}" for p in db_dir.iterdir() if p.is_dir()]
        else:
            return [p.name for p in db_dir.iterdir() if p.is_dir()]


    def get_table_schema(self, db_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get the schema of a table.
        
        Args:
            db_name: Name of the database
            table_name: Name of the table
            
        Returns:
            List of column definitions with name and type
        """
        pattern = self._build_parquet_glob(db_name, table_name)
        con = duckdb.connect()
        try:
            # Use DESCRIBE to get schema
            query = f"DESCRIBE SELECT * FROM read_parquet('{pattern}') LIMIT 0"
            result = con.execute(query).fetchall()
            schema = [{"name": row[0], "type": row[1]} for row in result]
            return schema
        finally:
            con.close()

    
    def get_table_exists(self, db_name: str, table_name: str) -> bool:
        """
        Check if a table exists.
        """
        if db_name == "default":
            table_dir = self.warehouse_path / table_name
        else:
            table_dir = self.warehouse_path / f"{db_name}.db" / table_name
        
        # Check if the directory exists and contains parquet files
        if not table_dir.exists() or not table_dir.is_dir():
            return False
        
        return self._has_parquet_under(table_dir)

    def sample_table(
        self, 
        db_name: str, 
        table_name: str, 
        limit: int = 100,
        offset: int = 0
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
                - schema: List of column definitions
                - row_count: Total number of rows in the table
        """
        pattern = self._build_parquet_glob(db_name, table_name)
        con = duckdb.connect()
        try:
            # Get total row count
            count_query = f"SELECT COUNT(*) AS cnt FROM read_parquet('{pattern}')"
            row_count = con.execute(count_query).fetchone()[0]
            
            # Get sample data
            sample_query = f"SELECT * FROM read_parquet('{pattern}') LIMIT ? OFFSET ?"
            df = con.execute(sample_query, [limit, offset]).fetch_df()
            
            # Get schema
            table_schema = self.get_table_schema(db_name, table_name)
            
            # Convert DataFrame to list of dicts
            data = df.to_dict(orient="records")
            # print(data)
            # Convert numpy arrays and types to native Python types for JSON serialization
            data = [self._convert_numpy_to_native(row) for row in data]
            
            return {
                "data": data,
                "table_schema": table_schema,
                "row_count": int(row_count),
                "limit": limit,
                "offset": offset
            }
        finally:
            con.close()

    def get_table_stats(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get statistics about a table.
        
        Args:
            db_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Dictionary containing table statistics
        """
        pattern = self._build_parquet_glob(db_name, table_name)
        con = duckdb.connect()
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) AS cnt FROM read_parquet('{pattern}')"
            row_count = con.execute(count_query).fetchone()[0]
            
            # Get schema
            schema_query = f"DESCRIBE SELECT * FROM read_parquet('{pattern}') LIMIT 0"
            schema_result = con.execute(schema_query).fetchall()
            
            return {
                "row_count": int(row_count),
                "column_count": len(schema_result),
                "columns": [{"name": row[0], "type": row[1]} for row in schema_result]
            }
        finally:
            con.close()

    def execute_query(self, query: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a custom SQL query against the warehouse.
        
        Args:
            query: SQL query to execute
            limit: Optional limit on number of rows returned
            
        Returns:
            Dictionary containing query results and schema
        """
        con = duckdb.connect()
        try:
            if limit:
                query = f"SELECT * FROM ({query}) AS subquery LIMIT {limit}"
            
            df = con.execute(query).fetch_df()
            table_schema = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
            data = df.to_dict(orient="records")
            
            # Convert numpy arrays and types to native Python types for JSON serialization
            data = [self._convert_numpy_to_native(row) for row in data]
            
            return {
                "data": data,
                "table_schema": table_schema,
                "row_count": len(data)
            }
        finally:
            con.close()

    def _build_parquet_glob(self, db_name: str, table_name: str) -> str:
        """
        Build a glob pattern to match all parquet files in a table.
        
        Args:
            db_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Glob pattern string
        """
        if db_name == "default":
            table_dir = self.warehouse_path / table_name
        else:
            table_dir = self.warehouse_path / f"{db_name}.db" / table_name
        
        # Use a recursive glob to capture all partitions
        return str(table_dir / "**" / "*.parquet")

    def _has_parquet_under(self, path: Path) -> bool:
        """
        Check if a directory contains any parquet files.
        
        Args:
            path: Path to check
            
        Returns:
            True if parquet files exist under the path
        """
        try:
            return any(path.rglob("*.parquet"))
        except Exception:
            return False
    
    def _convert_numpy_to_native(self, obj: Any) -> Any:
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
            return {key: self._convert_numpy_to_native(value) for key, value in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_numpy_to_native(item) for item in obj]
        else:
            return obj