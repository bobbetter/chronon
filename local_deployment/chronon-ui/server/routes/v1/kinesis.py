"""
API routes for Kinesis operations (produce, consume, inspect streams)
"""
import os
from enum import Enum
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from server.services.kinesis import KinesisClient
import logging

logger = logging.getLogger("uvicorn.error")

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/kinesis", tags=["kinesis"])

# Initialize the KinesisClient
kinesis_client = KinesisClient()


# Enums
class ShardIteratorType(str, Enum):
    """Enum for Kinesis shard iterator types."""
    TRIM_HORIZON = "TRIM_HORIZON"  # Start reading from the oldest record
    LATEST = "LATEST"  # Start reading from the newest record


# Request/Response models
class PutRecordsRequest(BaseModel):
    """Request model for putting records to a Kinesis stream."""
    records: List[Dict[str, Any]] = Field(
        ...,
        description="List of records to put to the stream. Each record should have 'data' and optionally 'partition_key'",
        example=[
            {"data": {"id": 1, "message": "Hello"}, "partition_key": "key1"},
            {"data": {"id": 2, "message": "World"}, "partition_key": "key2"}
        ]
    )


class PutRecordsResponse(BaseModel):
    """Response model for put records operation."""
    status: str = Field(..., description="Status: 'success' or 'error'")
    message: str = Field(..., description="Success or error message")
    records_pushed: Optional[int] = Field(None, description="Number of records successfully pushed")
    error: Optional[str] = Field(None, description="Error details if failed")


class GenerateTestRecordsRequest(BaseModel):
    """Request model for generating and putting test records."""
    count: int = Field(
        10,
        description="Number of test records to generate",
        example=10,
        gt=0,
        le=500
    )


class StreamSummaryResponse(BaseModel):
    """Response model for stream summary."""
    stream_name: str
    summary: Dict[str, Any]


class ShardsResponse(BaseModel):
    """Response model for listing shards."""
    stream_name: str
    shards: List[Dict[str, Any]]
    shard_count: int


class ConsumedRecord(BaseModel):
    """Model for a consumed record."""
    partition_key: str
    sequence_number: str
    approximate_arrival: Optional[str]
    data: Any


class ReadRecordsResponse(BaseModel):
    """Response model for reading records."""
    stream_name: str
    records: List[ConsumedRecord]
    record_count: int


class ListStreamsResponse(BaseModel):
    """Response model for listing streams."""
    streams: List[str]
    stream_count: int


@router.get("/streams", response_model=ListStreamsResponse)
async def list_streams(
    limit: Optional[int] = Query(
        None,
        description="Maximum number of streams to return (no limit if not specified)",
        gt=0,
        le=10000
    )
):
    """
    List all available Kinesis streams.
    
    Returns a list of stream names in the configured region/endpoint.
    Useful for discovering what streams are available for operations.
    
    Args:
        limit: Optional maximum number of streams to return
        
    Returns:
        ListStreamsResponse with list of stream names and count
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(f"Listing Kinesis streams (limit={limit})")
        streams = kinesis_client.list_streams(limit=limit)
        
        return ListStreamsResponse(
            streams=streams,
            stream_count=len(streams)
        )
    except Exception as e:
        logger.error(f"Unexpected error listing streams: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.post("/{stream_name}/records", response_model=PutRecordsResponse)
async def put_records(stream_name: str, request: PutRecordsRequest):
    """
    Put records to a Kinesis stream.
    
    This endpoint writes custom records to the specified Kinesis stream.
    Each record should have a 'data' field (which will be JSON-encoded) and
    optionally a 'partition_key' field (defaults to 'default-key' if not provided).
    
    Args:
        stream_name: The name of the Kinesis stream
        request: The records to put to the stream
        
    Returns:
        PutRecordsResponse with success/error status and record count
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        import json
        
        logger.info(f"Putting {len(request.records)} records to stream '{stream_name}'")
        
        # Transform the records into the format expected by kinesis_client
        formatted_records = []
        for idx, record in enumerate(request.records):
            data = record.get("data", {})
            partition_key = record.get("partition_key", f"default-key-{idx}")
            
            formatted_records.append({
                "Data": json.dumps(data).encode("utf-8"),
                "PartitionKey": partition_key
            })
        
        records_pushed = kinesis_client.put_records(stream_name, formatted_records)
        
        return PutRecordsResponse(
            status="success",
            message=f"Successfully pushed {records_pushed} records to '{stream_name}'",
            records_pushed=records_pushed
        )
    except Exception as e:
        logger.error(f"Unexpected error putting records to '{stream_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.post("/{stream_name}/test-records", response_model=PutRecordsResponse)
async def generate_and_put_test_records(
    stream_name: str, 
    request: GenerateTestRecordsRequest
):
    """
    Generate and put test records to a Kinesis stream.
    
    This endpoint generates a specified number of test records with predictable
    payloads and writes them to the stream. Useful for testing and development.
    
    Args:
        stream_name: The name of the Kinesis stream
        request: Request specifying how many test records to generate
        
    Returns:
        PutRecordsResponse with success/error status and record count
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(f"Generating and putting {request.count} test records to stream '{stream_name}'")
        
        # Generate test records
        records = kinesis_client.build_records(request.count)
        
        # Put them to the stream
        records_pushed = kinesis_client.put_records(stream_name, records)
        
        return PutRecordsResponse(
            status="success",
            message=f"Successfully pushed {records_pushed} test records to '{stream_name}'",
            records_pushed=records_pushed
        )
    except Exception as e:
        logger.error(f"Unexpected error with test records for '{stream_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.get("/{stream_name}/summary", response_model=StreamSummaryResponse)
async def get_stream_summary(stream_name: str):
    """
    Get summary information for a Kinesis stream.
    
    Returns metadata about the stream including ARN, status, shard count,
    retention period, and other stream-level information.
    
    Args:
        stream_name: The name of the Kinesis stream
        
    Returns:
        StreamSummaryResponse with stream summary details
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(f"Fetching summary for stream '{stream_name}'")
        
        summary = kinesis_client.get_stream_summary(stream_name)
        
        return StreamSummaryResponse(
            stream_name=stream_name,
            summary=summary
        )
    except Exception as e:
        logger.error(f"Unexpected error getting summary for '{stream_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.get("/{stream_name}/shards", response_model=ShardsResponse)
async def list_shards(stream_name: str):
    """
    List all shards for a Kinesis stream.
    
    Returns information about all shards in the stream, including shard IDs,
    sequence number ranges, and parent/child relationships.
    
    Args:
        stream_name: The name of the Kinesis stream
        
    Returns:
        ShardsResponse with list of shards and count
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(f"Listing shards for stream '{stream_name}'")
        
        shards = kinesis_client.list_shards(stream_name)
        
        return ShardsResponse(
            stream_name=stream_name,
            shards=shards,
            shard_count=len(shards)
        )
    except Exception as e:
        logger.error(f"Unexpected error listing shards for '{stream_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.get("/{stream_name}/records", response_model=ReadRecordsResponse)
async def read_records(
    stream_name: str,
    iterator_type: ShardIteratorType,
    limit: int = 10
):
    """
    Read records from a Kinesis stream.
    
    Reads and decodes records from all shards in the stream. Records are returned
    with their partition key, sequence number, approximate arrival time, and decoded data.
    
    Args:
        stream_name: The name of the Kinesis stream
        iterator_type: Where to start reading from (TRIM_HORIZON or LATEST)
        limit: Maximum number of records to return
        
    Returns:
        ReadRecordsResponse with list of consumed records
        
    Raises:
        HTTPException: If the operation fails
    """
    try:
        logger.info(
            f"Reading up to {limit} records from stream '{stream_name}' "
            f"(iterator_type={iterator_type})"
        )
        
        records = kinesis_client.read_records(
            stream_name=stream_name,
            iterator_type=iterator_type.value,
            limit=limit
        )
        
        # Convert to Pydantic models
        consumed_records = [ConsumedRecord(**record) for record in records]
        
        return ReadRecordsResponse(
            stream_name=stream_name,
            records=consumed_records,
            record_count=len(consumed_records)
        )
    except Exception as e:
        logger.error(f"Unexpected error reading records from '{stream_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@router.get("/health")
async def health_check():
    """
    Health check endpoint for Kinesis service.
    
    Verifies that the Kinesis client can be initialized and is ready to
    accept requests.
    
    Returns:
        Status information about the Kinesis client
    """
    try:
        # Simple check to see if client is initialized
        return {
            "status": "healthy",
            "kinesis_client": "initialized",
            "endpoint_url": kinesis_client.client._endpoint.host,
            "region": kinesis_client.client.meta.region_name
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

