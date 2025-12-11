import json
import os
import re
import logging
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from server.services.datascanner_base import DataScannerBase
from server.services.dynamodb import DynamoDBClient
from server.services.kinesis import KinesisClient
from server.config import ComputeEngine
from pathlib import Path

logger = logging.getLogger("uvicorn.error")
RAW_DATA_DB_NAME = "data" # TODO: Make this dynamic



class NodeTypes(Enum):
    # batch data nodes
    RAW_DATA = "raw_data"
    BACKFILL_GROUP_BY = "backfill_group_by"
    BACKFILL_JOIN = "backfill_join"
    PRE_COMPUTED_UPLOAD = "pre_computed_upload"

    # streaming data nodes
    EVENT_STREAM = "event_stream"

    # configuration nodes
    GROUP_BY = "group_by"
    JOIN = "join"

    # online data nodes
    BATCH_UPLOADED = "batch_uploaded"
    STREAMING_INGESTED = "streaming_ingested"


class NodeTypesVisual(Enum):
    BATCH_DATA = "batch-data"
    ONLINE_DATA = "online-data"
    STREAMING_DATA = "streaming-data"
    CONFIGURATION = "configuration"


class Node:
    def __init__(
        self,
        name: str,
        type: Union[NodeTypes, str],
        type_visual: Union[NodeTypesVisual, str],
        exists: bool,
        actions: Optional[List[str]],
        config_file_path: Optional[str] = None,
    ):
        self.name = name
        self.type = type.value if isinstance(type, Enum) else type
        self.type_visual = (
            type_visual.value if isinstance(type_visual, Enum) else type_visual
        )
        self.exists = exists
        self.actions = actions
        self.config_file_path = config_file_path

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "type_visual": self.type_visual,
            "exists": self.exists,
            "actions": self.actions,
            "config_file_path": self.config_file_path,
        }


class Edge:
    def __init__(self, source: str, target: str, exists: bool):
        self.source = source
        self.target = target
        self.exists = exists

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "exists": self.exists,
        }


class Graph:
    def __init__(self):
        self.nodes: List[Node] = []
        self.edges: List[Edge] = []

    def add_node(self, node: Node) -> None:
        self.nodes.append(node)

    def add_edge(self, edge: Edge) -> None:
        self.edges.append(edge)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
        }


def _underscore_name(name: str) -> str:
    return name.replace(".", "_")


class _ParseContext:
    """
    Thread-safe context for a single parse operation.
    
    This holds all the mutable state needed during parsing, ensuring that
    concurrent parse() calls don't interfere with each other.
    """
    
    def __init__(
        self,
        datascanner: Optional[DataScannerBase],
        dynamodb_client: Optional[DynamoDBClient] = None,
    ):
        self.graph = Graph()
        self.seen_nodes: Set[str] = set()
        self.seen_edges: Set[Tuple[str, str]] = set()
        self.datascanner = datascanner
        self.dynamodb_client = dynamodb_client

    def add_node_once(self, node: Node) -> None:
        if node.name in self.seen_nodes:
            return
        self.graph.add_node(node)
        self.seen_nodes.add(node.name)

    def add_edge_once(self, edge: Edge) -> None:
        key = (edge.source, edge.target)
        if key in self.seen_edges:
            return
        self.graph.add_edge(edge)
        self.seen_edges.add(key)

    def get_batch_data_exists(self, table_name: str) -> bool:
        if self.datascanner is None:
            return False

        database_name = RAW_DATA_DB_NAME
        table = table_name
        if "." in table_name:
            database_name, table = table_name.split(".", 1)

        return self.datascanner.get_table_exists(database_name, table)

    def get_online_table_exists(self, dataset_name: str) -> bool:
        """
        Check if an online table exists in DynamoDB.
        
        Args:
            dataset_name: The dataset/config name (will be sanitized to DynamoDB table name)
            
        Returns:
            True if the DynamoDB table exists, False otherwise
        """
        if self.dynamodb_client is None:
            return False

        try:
            return self.dynamodb_client.table_exists(dataset_name, sanitize=True)
        except Exception as e:
            logger.warning(f"Error checking online table existence for '{dataset_name}': {e}")
            return False


class GraphParser:
    """This parses all configuration files and creates a "full graph" of configuration files,
    raw data nodes, streaming data nodes, online data nodes, and edges between them.
    """

    IGNORE_FILES = ["schema.v1__1"]

    def __init__(
        self,
        directory_path_gbs: Path,
        directory_path_joins: Path = None,
        datascanner_local: Optional[DataScannerBase] = None,
        datascanner_remote: Optional[DataScannerBase] = None,
        dynamodb_client_local: Optional[DynamoDBClient] = None,
        dynamodb_client_remote: Optional[DynamoDBClient] = None,
        kinesis_client: KinesisClient = None,
    ):
        self._directory_path_gbs = directory_path_gbs
        self._directory_path_joins = directory_path_joins
        self._datascanner_local = datascanner_local
        self._datascanner_remote = datascanner_remote
        self._dynamodb_client_local = dynamodb_client_local
        self._dynamodb_client_remote = dynamodb_client_remote
        self._kinesis_client = kinesis_client

    def _get_datascanner(self, compute_engine: ComputeEngine) -> Optional[DataScannerBase]:
        """Get the appropriate datascanner based on compute engine."""
        if compute_engine == ComputeEngine.REMOTE:
            return self._datascanner_remote
        return self._datascanner_local

    def _get_dynamodb_client(self, compute_engine: ComputeEngine) -> Optional[DynamoDBClient]:
        """Get the appropriate DynamoDB client based on compute engine."""
        if compute_engine == ComputeEngine.REMOTE:
            return self._dynamodb_client_remote
        return self._dynamodb_client_local

    def _get_stream_name(self, config_data: Dict[str, Any]) -> Union[Tuple[str, bool], None]:
        # Not sure what the case for multiple sources is in Chronon
        sources = config_data["sources"][0]
        
        # Check if this is an events source (not entities)
        if "events" not in sources:
            return (None, False)
            
        events = sources["events"]

        if "topic" in events:
            topic = events["topic"]

            # Handle both formats:
            # 1. "kinesis://login-events/fields=..."
            # 2. "login-events/fields=..."

            # Strip streaming bus prefix if present
            if topic.startswith("kinesis://"):
                topic = topic[len("kinesis://") :]
            elif topic.startswith("kafka://"):
                topic = topic[len("kafka://") :]

            # Get the stream name (first part before /)
            stream_name = topic.split("/")[0]
            logger.info(f"Found event stream name: {stream_name}")
            if self._kinesis_client:
                exists = stream_name in self._kinesis_client.list_streams()
                logger.info(f"Stream exists: {exists}")
            else:
                exists = False
            return (stream_name, exists)
        else:
            return (None, False)

    def _parse_group_by_config(
        self,
        ctx: _ParseContext,
        config_data: Dict[str, Any],
        config_file_path: Optional[str] = None,
    ) -> None:
        conf_name: str = config_data["metaData"]["name"]
        sources = config_data["sources"][0]
        if "events" in sources:
            raw_table_name = sources["events"]["table"]
        elif "entities" in sources:
            raw_table_name = sources["entities"]["snapshotTable"]
        else:
            raise ValueError(f"Invalid source type: {sources}")

        team_name: str = config_data["metaData"]["team"]
        backfill_name = f"{team_name}.{_underscore_name(conf_name)}"
        upload_name = f"{team_name}.{_underscore_name(conf_name)}__upload"
        online_data_batch_name = f"{_underscore_name(conf_name)}_batch"
        online_data_streaming_table_name = f"{_underscore_name(conf_name)}_streaming"

        nodes = [
            Node(
                conf_name,
                NodeTypes.GROUP_BY,
                NodeTypesVisual.CONFIGURATION,
                True,
                ["backfill", "pre-compute-upload", "show-online-data"],
                config_file_path,
            ),
            Node(
                raw_table_name,
                NodeTypes.RAW_DATA,
                NodeTypesVisual.BATCH_DATA,
                ctx.get_batch_data_exists(raw_table_name),
                ["show"],
                None,
            ),
            Node(
                backfill_name,
                NodeTypes.BACKFILL_GROUP_BY,
                NodeTypesVisual.BATCH_DATA,
                ctx.get_batch_data_exists(backfill_name),
                ["show"],
                None,
            ),
            Node(
                upload_name,
                NodeTypes.PRE_COMPUTED_UPLOAD,
                NodeTypesVisual.BATCH_DATA,
                ctx.get_batch_data_exists(upload_name),
                ["show", "upload-to-kv"],
                config_file_path,
            ),
            Node(
                online_data_batch_name,
                NodeTypes.BATCH_UPLOADED,
                NodeTypesVisual.ONLINE_DATA,
                ctx.get_online_table_exists(online_data_batch_name),
                None,
                None,
            ),
        ]

        input_stream_name, stream_exists = self._get_stream_name(config_data)
        if input_stream_name:
            nodes.append(
                Node(
                    input_stream_name,
                    NodeTypes.EVENT_STREAM,
                    NodeTypesVisual.STREAMING_DATA,
                    stream_exists,
                    None,
                    None,
                )
            )
            nodes.append(
                Node(
                    online_data_streaming_table_name,
                    NodeTypes.STREAMING_INGESTED,
                    NodeTypesVisual.ONLINE_DATA,
                    ctx.get_online_table_exists(online_data_streaming_table_name),
                    None,
                    None,
                )
            )

        for node in nodes:
            ctx.add_node_once(node)

        edges = [
            Edge(raw_table_name, conf_name, True),
            Edge(conf_name, backfill_name, True),
            Edge(conf_name, upload_name, True),
            Edge(upload_name, online_data_batch_name, True),
        ]

        if input_stream_name:
            edges.append(Edge(input_stream_name, conf_name, True))
        
        if online_data_streaming_table_name:
            edges.append(Edge(conf_name, online_data_streaming_table_name, True))

        for edge in edges:
            ctx.add_edge_once(edge)

    def _parse_join_config(
        self,
        ctx: _ParseContext,
        config_data: Dict[str, Any],
        config_file_path: Optional[str] = None,
    ) -> None:
        conf_name: str = config_data["metaData"]["name"]
        join_parts = config_data["joinParts"]
        team_name: str = config_data["metaData"]["team"]
        training_data_set_name = f"{team_name}.{_underscore_name(conf_name)}"
        left_table_name = config_data["left"]["events"]["table"]
        nodes = [
            Node(
                left_table_name,
                NodeTypes.RAW_DATA,
                NodeTypesVisual.BATCH_DATA,
                ctx.get_batch_data_exists(left_table_name),
                ["show"],
                None,
            ),
            Node(
                conf_name,
                NodeTypes.JOIN,
                NodeTypesVisual.CONFIGURATION,
                True,
                ["backfill", "show-online-data"],
                config_file_path,
            ),
            Node(
                training_data_set_name,
                NodeTypes.BACKFILL_JOIN,
                NodeTypesVisual.BATCH_DATA,
                ctx.get_batch_data_exists(training_data_set_name),
                ["show"],
                None,
            ),
        ]
        for node in nodes:
            ctx.add_node_once(node)

        ctx.add_edge_once(
            Edge(source=left_table_name, target=conf_name, exists=True))

        ctx.add_edge_once(
            Edge(source=conf_name, target=training_data_set_name, exists=True)
        )

        for join_part in join_parts:
            group_by_name = join_part["groupBy"]["metaData"]["name"]
            edge = Edge(source=group_by_name, target=conf_name, exists=True)
            ctx.add_edge_once(edge)

    def _get_short_config_file_path(self, config_file_path: str) -> str:
        """/app/server/chronon_config/compiled/group_bys/quickstart/users.v1__1
        -> compiled/group_bys/quickstart/users.v1__1"""
        match = re.search(r"(compiled/.*)$", config_file_path)
        if match:
            return match.group(1)
        return config_file_path

    def _add_orphan_raw_data_nodes(self, ctx: _ParseContext) -> None:
        """Add raw data nodes that have not been referenced in any configuration files yet."""
        if ctx.datascanner is None:
            return
        all_raw_data_nodes = ctx.datascanner.list_tables(
            db_name=RAW_DATA_DB_NAME, with_db_name=True
        )
        raw_data_nodes_from_conf = [
            x.name for x in ctx.graph.nodes if x.type == NodeTypes.RAW_DATA.value
        ]

        for raw_data_node in all_raw_data_nodes:
            if raw_data_node not in raw_data_nodes_from_conf:
                ctx.graph.add_node(
                    Node(
                        raw_data_node,
                        NodeTypes.RAW_DATA,
                        NodeTypesVisual.BATCH_DATA,
                        True,
                        ["show"],
                        None,
                    )
                )

    def _add_orphan_streaming_data_nodes(self, ctx: _ParseContext) -> None:
        if self._kinesis_client is None:
            return
        all_streaming_data_nodes = self._kinesis_client.list_streams()
        streaming_data_nodes_from_conf = [
            x.name for x in ctx.graph.nodes if x.type == NodeTypes.EVENT_STREAM.value
        ]

        for streaming_data_node in all_streaming_data_nodes:
            if streaming_data_node not in streaming_data_nodes_from_conf:
                logger.info(f"Adding orphan streaming data node: {streaming_data_node}")
                ctx.graph.add_node(
                    Node(
                        streaming_data_node,
                        NodeTypes.EVENT_STREAM,
                        NodeTypesVisual.STREAMING_DATA,
                        True,
                        None,
                        None,
                    )
                )

    def _iter_compiled_files(
        self, directory_path: Optional[str]
    ) -> Iterable[Tuple[str, str]]:
        """Yield (file_path, short_config_file_path) for compiled config files."""
        if not os.path.isdir(directory_path):
            logger.warning(f"Directory not found for parsing: {directory_path}")
            return []
        for entry in sorted(os.listdir(directory_path)):
            file_path = os.path.join(directory_path, entry)
            if not os.path.isfile(file_path) or entry in self.IGNORE_FILES:
                continue

            yield file_path, self._get_short_config_file_path(file_path)

    def parse(
        self, team_name: str, compute_engine: ComputeEngine = ComputeEngine.LOCAL
    ) -> Dict[str, Any]:
        """
        Parse configuration files and build a graph.
        
        This method is thread-safe - all mutable state is kept in a local _ParseContext.
        
        Args:
            team_name: The team name to parse configurations for
            compute_engine: Which compute engine to use for checking data existence
            
        Returns:
            Dictionary representation of the graph with nodes and edges
        """
        # Create a thread-local context for this parse operation
        datascanner = self._get_datascanner(compute_engine)
        dynamodb_client = self._get_dynamodb_client(compute_engine)
        ctx = _ParseContext(datascanner, dynamodb_client)

        if self._directory_path_gbs:
            for file_path, short_path in self._iter_compiled_files(
                str(self._directory_path_gbs / team_name)
            ):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        config_data = json.load(f)
                    self._parse_group_by_config(ctx, config_data, short_path)
                except Exception as e:
                    logger.error(f"Skipping file {file_path}: {e}")

        if self._directory_path_joins:
            for file_path, short_path in self._iter_compiled_files(
                str(self._directory_path_joins / team_name)
            ):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        config_data = json.load(f)
                    self._parse_join_config(ctx, config_data, short_path)
                except Exception as e:
                    logger.debug(f"Skipping file {file_path}: {e}")

        self._add_orphan_raw_data_nodes(ctx)
        self._add_orphan_streaming_data_nodes(ctx)
        return ctx.graph.to_dict()
