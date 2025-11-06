import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, TypedDict

import boto3


class _RecordSpec(TypedDict):
    Data: bytes
    PartitionKey: str


class _ConsumedRecord(TypedDict):
    partition_key: str
    sequence_number: str
    approximate_arrival: Optional[str]
    data: Any


ENDPOINT_URL = os.environ.get("KINESIS_ENDPOINT_URL", "http://localstack:4566")
REGION_NAME = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "local")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "local")
SESSION_TOKEN = None

class KinesisClient:

    def __init__(
        self, 
        region_name: str = REGION_NAME, 
        endpoint_url: str = ENDPOINT_URL, 
        access_key: str = ACCESS_KEY, 
        secret_key: str = SECRET_KEY, 
        session_token: str = SESSION_TOKEN
    ) -> None:
        self.client = boto3.client(
        "kinesis",
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    def build_records(self, count: int) -> List[_RecordSpec]:
        """Generate `count` demo records with predictable payloads."""

        records: List[_RecordSpec] = []
        for idx in range(count):
            payload = {
                "id": idx,
                "message": f"Hello from test_kinesis #{idx}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            records.append(
                {
                    "Data": json.dumps(payload).encode("utf-8"),
                    "PartitionKey": f"partition-{idx % 5}",
                }
            )

        return records

    def put_records(self, stream_name: str, records: Iterable[_RecordSpec]) -> int:
        """Put records to a Kinesis stream and return the count of successfully published records."""
        response = self.client.put_records(
            Records=list(records), 
            StreamName=stream_name
        )

        failed = response.get("FailedRecordCount", 0)
        if failed:
            raise Exception(
                f"Completed with {failed} failed record(s). Response: {json.dumps(response, indent=2)}"
            )

        records_pushed = len(response['Records'])
        print(f"Successfully pushed {records_pushed} record(s) to '{stream_name}'.")
        return records_pushed

    def list_streams(self, limit: Optional[int] = None) -> List[str]:
        """Return the list of available Kinesis streams using pagination as needed."""

        streams: List[str] = []
        exclusive_start_stream_name: Optional[str] = None

        while True:
            params: Dict[str, Any] = {}
            if limit:
                params["Limit"] = min(limit - len(streams), 100) if len(streams) < limit else 100
            if exclusive_start_stream_name:
                params["ExclusiveStartStreamName"] = exclusive_start_stream_name

            response = self.client.list_streams(**params)
            stream_names = response.get("StreamNames", [])
            streams.extend(stream_names)

            # Check if we've reached the desired limit
            if limit and len(streams) >= limit:
                streams = streams[:limit]
                break

            # Check if there are more streams to fetch
            has_more_streams = response.get("HasMoreStreams", False)
            if not has_more_streams or not stream_names:
                break

            # Use the last stream name for pagination
            exclusive_start_stream_name = stream_names[-1]

        return streams

    def get_stream_summary(self, stream_name: str) -> Dict[str, Any]:
        """Return the summary information for a given Kinesis stream."""
        response = self.client.describe_stream_summary(StreamName=stream_name)
        return response.get("StreamDescriptionSummary", {})

    def list_shards(self, stream_name: str) -> List[Dict[str, Any]]:
        """Return the list of shards for `stream_name` using pagination as needed."""
        shards: List[Dict[str, Any]] = []
        next_token: Optional[str] = None

        while True:
            params: Dict[str, Any]
            if next_token:
                params = {"NextToken": next_token}
            else:
                params = {"StreamName": stream_name}

            response = self.client.list_shards(**params)
            shards.extend(response.get("Shards", []))
            next_token = response.get("NextToken")

            if not next_token:
                break

        return shards

    def read_records(
        self,
        stream_name: str,
        iterator_type: str = "TRIM_HORIZON",
        limit: int = 25,
    ) -> List[_ConsumedRecord]:
        """Read and decode up to `limit` records from all shards in `stream_name`."""

        if limit <= 0:
            return []

        shards = self.list_shards(stream_name)
        if not shards:
            return []

        results: List[_ConsumedRecord] = []

        for shard in shards:
            shard_id = shard.get("ShardId")
            if not shard_id:
                continue

            iterator = self._get_shard_iterator(
                stream_name=stream_name,
                shard_id=shard_id,
                iterator_type=iterator_type,
            )

            while iterator and len(results) < limit:
                response = self.client.get_records(
                    ShardIterator=iterator,
                    Limit=min(limit - len(results), 10_000),
                )

                for raw_record in response.get("Records", []):
                    results.append(self._decode_record(raw_record))
                    if len(results) >= limit:
                        break

                iterator = response.get("NextShardIterator")

                if not response.get("Records"):
                    break

            if len(results) >= limit:
                break

        return results

    def _get_shard_iterator(
        self,
        stream_name: str,
        shard_id: str,
        iterator_type: str,
    ) -> Optional[str]:
        """Get a shard iterator for reading records from a specific shard."""
        response = self.client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType=iterator_type,
        )
        return response.get("ShardIterator")

    def _decode_record(self, record: Dict[str, Any]) -> _ConsumedRecord:
        data = record.get("Data", b"")
        if isinstance(data, (bytes, bytearray)):
            decoded = data.decode("utf-8")
        else:  # pragma: no cover - defensive
            decoded = str(data)

        try:
            payload: Any = json.loads(decoded)
        except json.JSONDecodeError:
            payload = decoded

        arrival = record.get("ApproximateArrivalTimestamp")
        arrival_str = arrival.isoformat() if isinstance(arrival, datetime) else None

        return {
            "partition_key": record.get("PartitionKey", ""),
            "sequence_number": record.get("SequenceNumber", ""),
            "approximate_arrival": arrival_str,
            "data": payload,
        }

    def create_stream(self, stream_name: str, shard_count: int = 1) -> Dict[str, Any]:
        """Create a new Kinesis stream with the specified number of shards."""
        response = self.client.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        
        # Wait for the stream to become active
        waiter = self.client.get_waiter('stream_exists')
        waiter.wait(StreamName=stream_name, WaiterConfig={'Delay': 1, 'MaxAttempts': 60})
        
        print(f"Successfully created stream '{stream_name}' with {shard_count} shard(s).")
        return response

    def delete_stream(self, stream_name: str, enforce_consumer_deletion: bool = False) -> Dict[str, Any]:
        """Delete a Kinesis stream."""
        response = self.client.delete_stream(
            StreamName=stream_name,
            EnforceConsumerDeletion=enforce_consumer_deletion
        )
        
        # Wait for deletion to complete
        waiter = self.client.get_waiter('stream_not_exists')
        waiter.wait(StreamName=stream_name, WaiterConfig={'Delay': 1, 'MaxAttempts': 60})
        
        print(f"Successfully deleted stream '{stream_name}'.")
        return response

    def clear_stream(self, stream_name: str, shard_count: int = 1, enforce_consumer_deletion: bool = False) -> None:
        """Delete and recreate a stream to effectively clear all records."""
        # Delete the stream
        self.delete_stream(stream_name, enforce_consumer_deletion)
        
        # Recreate the stream
        self.create_stream(stream_name, shard_count)
        
        print(f"Successfully cleared stream '{stream_name}' (deleted and recreated with {shard_count} shard(s)).")