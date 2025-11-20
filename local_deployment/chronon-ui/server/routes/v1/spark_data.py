import os
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, Field
from server.services.datascanner import DataScanner

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/spark-data", tags=["spark-data"])
logger = logging.getLogger("uvicorn.error")


# Pydantic models for request/response validation
class ColumnSchema(BaseModel):
    name: str
    type: str


class DatabaseResponse(BaseModel):
    databases: List[str]


class TablesResponse(BaseModel):
    database: str
    tables: List[str]


class TableStatsResponse(BaseModel):
    database: str
    table: str
    row_count: int
    column_count: int
    columns: List[ColumnSchema]


class TableSampleResponse(BaseModel):
    database: str
    table: str
    data: List[Dict[str, Any]]
    table_schema: List[ColumnSchema]
    row_count: int
    limit: int
    offset: int


class QueryRequest(BaseModel):
    query: str
    limit: Optional[int] = Field(default=None, ge=1, le=10000)


class QueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    table_schema: List[ColumnSchema]
    row_count: int


def get_datascanner(request: Request) -> DataScanner:
    """Dependency to access the singleton DataScanner from app state."""
    return request.app.state.datascanner


@router.get("/databases", response_model=DatabaseResponse)
def list_databases(scanner: DataScanner = Depends(get_datascanner)):
    """
    List all available databases in the Spark warehouse.

    Returns:
        DatabaseResponse: List of database names
    """
    try:
        databases = scanner.list_databases()
        logger.info(f"Found {len(databases)} databases")
        return DatabaseResponse(databases=databases)
    except Exception as e:
        logger.error(f"Error listing databases: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to list databases: {str(e)}"
        )


@router.get("/databases/{db_name}/tables", response_model=TablesResponse)
def list_tables(db_name: str, scanner: DataScanner = Depends(get_datascanner)):
    """
    List all tables in a specific database.

    Args:
        db_name: Name of the database

    Returns:
        TablesResponse: List of table names in the database
    """
    try:
        tables = scanner.list_tables(db_name)
        logger.info(f"Found {len(tables)} tables in database '{db_name}'")
        return TablesResponse(database=db_name, tables=tables)
    except Exception as e:
        logger.error(
            f"Error listing tables in database '{db_name}': {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@router.get(
    "/databases/{db_name}/tables/{table_name}/stats", response_model=TableStatsResponse
)
def get_table_stats(
    db_name: str, table_name: str, scanner: DataScanner = Depends(get_datascanner)
):
    """
    Get statistics for a specific table.

    Args:
        db_name: Name of the database
        table_name: Name of the table

    Returns:
        TableStatsResponse: Table statistics including row count and schema
    """
    try:
        stats = scanner.get_table_stats(db_name, table_name)
        logger.info(
            f"Retrieved stats for {db_name}.{table_name}: {stats['row_count']} rows, {stats['column_count']} columns"
        )
        return TableStatsResponse(database=db_name, table=table_name, **stats)
    except Exception as e:
        logger.error(
            f"Error getting stats for {db_name}.{table_name}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to get table stats: {str(e)}"
        )


@router.get(
    "/databases/{db_name}/tables/{table_name}/sample",
    response_model=TableSampleResponse,
)
def sample_table(
    db_name: str,
    table_name: str,
    limit: int = Query(
        default=100, ge=1, le=1000, description="Maximum number of rows to return"
    ),
    offset: int = Query(default=0, ge=0, description="Number of rows to skip"),
    scanner: DataScanner = Depends(get_datascanner),
):
    """
    Get a sample of rows from a specific table.

    Args:
        db_name: Name of the database
        table_name: Name of the table
        limit: Maximum number of rows to return (default: 100, max: 1000)
        offset: Number of rows to skip (default: 0)

    Returns:
        TableSampleResponse: Sample data with schema and metadata
    """
    try:
        result = scanner.sample_table(db_name, table_name, limit=limit, offset=offset)
        # logger.info(f"Retrieved {len(result['data'])} rows from {db_name}.{table_name} (limit={limit}, offset={offset})")
        # logger.info(f"Result: {result['data'][0]['value_bytes']}")
        return TableSampleResponse(database=db_name, table=table_name, **result)
    except Exception as e:
        logger.error(f"Error sampling {db_name}.{table_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to sample table: {str(e)}")


@router.get(
    "/databases/{db_name}/tables/{table_name}/schema", response_model=List[ColumnSchema]
)
def get_table_schema(
    db_name: str, table_name: str, scanner: DataScanner = Depends(get_datascanner)
):
    """
    Get the schema of a specific table.

    Args:
        db_name: Name of the database
        table_name: Name of the table

    Returns:
        List[ColumnSchema]: List of column definitions
    """
    try:
        schema = scanner.get_table_schema(db_name, table_name)
        logger.info(
            f"Retrieved schema for {db_name}.{table_name}: {len(schema)} columns"
        )
        return schema
    except Exception as e:
        logger.error(
            f"Error getting schema for {db_name}.{table_name}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to get table schema: {str(e)}"
        )


@router.post("/query", response_model=QueryResponse)
def execute_query(
    request: QueryRequest, scanner: DataScanner = Depends(get_datascanner)
):
    """
    Execute a custom SQL query against the warehouse.

    Args:
        request: Query request containing the SQL query and optional limit

    Returns:
        QueryResponse: Query results with data and schema
    """
    try:
        result = scanner.execute_query(request.query, limit=request.limit)
        logger.info(f"Executed custom query, returned {result['row_count']} rows")
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")
