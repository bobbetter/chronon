import json
import os
import re
import logging
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from server.services.datascanner import DataScanner

logger = logging.getLogger("uvicorn.error")
HARDCODED_DB_NAME = "data" # TODO: Make this dynamic

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
        config_file_path: Optional[str]=None
    ):
        self.name = name
        self.type = type.value if isinstance(type, Enum) else type
        self.type_visual = type_visual.value if isinstance(type_visual, Enum) else type_visual
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


class GraphParser:
    """This parses all configuration files and creates a "full graph" of configuration files, 
    raw data nodes, streaming data nodes, online data nodes, and edges between them.
    """
    IGNORE_FILES = ["schema.v1__1"]
    def __init__(self, directory_path_gbs: str, directory_path_joins: str = None, datascanner: DataScanner = None):
        self._directory_path_gbs = directory_path_gbs
        self._directory_path_joins = directory_path_joins
        self._datascanner = datascanner
        self.graph = Graph()
        self.seen_nodes: set = set()
        self.seen_edges: set = set()

    def _get_batch_data_exists(self, table_name: str) -> bool:
        # If no datascanner is provided, we can't check existence
        if self._datascanner is None:
            return False

        database_name = HARDCODED_DB_NAME
        table = table_name
        if "." in table_name:
            database_name, table = table_name.split(".", 1)

        return self._datascanner.get_table_exists(database_name, table)

    def _add_node_once(self, node: Node) -> None:
        if node.name in self.seen_nodes:
            return

        self.graph.add_node(node)
        self.seen_nodes.add(node.name)

    def _add_edge_once(self, edge: Edge) -> None:
        key = (edge.source, edge.target)
        if key in self.seen_edges:
            return

        self.graph.add_edge(edge)
        self.seen_edges.add(key)

    def _add_compiled_to_graph_gbs(self, compiled_data: Dict[str, Any], config_file_path: Optional[str]=None) -> None:
        conf_name: str = compiled_data["metaData"]["name"]
        sources = compiled_data["sources"][0]
        if "events" in sources:
            raw_table_name = sources["events"]["table"]
        elif "entities" in sources:
            raw_table_name = sources["entities"]["snapshotTable"]
        else:
            raise ValueError(f"Invalid source type: {sources}")

        team_name: str = compiled_data["metaData"]["team"]
        backfill_name = f"{team_name}.{_underscore_name(conf_name)}"
        upload_name = f"{team_name}.{_underscore_name(conf_name)}__upload"
        online_data_batch_name = f"{_underscore_name(conf_name)}_batch"

        try:
            stream_event_name: str = compiled_data["sources"][0]["events"]["topic"]
            stream_event_name = stream_event_name.split("/")[0]
            online_data_stream_name = f"{team_name}.{_underscore_name(stream_event_name)}_streaming"
        except Exception as e:
            stream_event_name = None

        nodes = [
            Node(conf_name, NodeTypes.GROUP_BY, NodeTypesVisual.CONFIGURATION, True, ["backfill", "pre-compute-upload", "show-online-data"], config_file_path),
            Node(raw_table_name, NodeTypes.RAW_DATA, NodeTypesVisual.BATCH_DATA, self._get_batch_data_exists(raw_table_name), ["show"], None),
            Node(backfill_name, NodeTypes.BACKFILL_GROUP_BY, NodeTypesVisual.BATCH_DATA, self._get_batch_data_exists(backfill_name), ["show"], None),
            Node(upload_name, NodeTypes.PRE_COMPUTED_UPLOAD, NodeTypesVisual.BATCH_DATA, self._get_batch_data_exists(upload_name), ["show", "upload-to-kv"], config_file_path),
            Node(online_data_batch_name, NodeTypes.BATCH_UPLOADED, NodeTypesVisual.ONLINE_DATA, self._get_batch_data_exists(upload_name), None, None),
        ]

        if stream_event_name:
            nodes.append(
                Node(stream_event_name, NodeTypes.EVENT_STREAM, NodeTypesVisual.STREAMING_DATA, True, None, None)
            )
            nodes.append(
                Node(online_data_stream_name, NodeTypes.STREAMING_INGESTED, NodeTypesVisual.ONLINE_DATA,  True, None, None)
            )

        for node in nodes:
            self._add_node_once(node)

        edges = [
            Edge(raw_table_name, conf_name, True),
            Edge(conf_name, backfill_name, True),
            Edge(conf_name, upload_name, True),
            Edge(upload_name, online_data_batch_name, True),
        ]

        if stream_event_name:
            edges.append(Edge(stream_event_name, conf_name, True))
            edges.append(Edge(conf_name, online_data_stream_name, True))

        for edge in edges:
            self._add_edge_once(edge)

    def _add_compiled_to_graph_joins(self, compiled_data: Dict[str, Any], config_file_path: Optional[str]=None) -> None:
        conf_name: str = compiled_data["metaData"]["name"]
        join_parts = compiled_data["joinParts"]
        team_name: str = compiled_data["metaData"]["team"]
        training_data_set_name = f"{team_name}.{_underscore_name(conf_name)}"
        left_table_name = compiled_data["left"]["events"]["table"]
        nodes = [
            Node(left_table_name, NodeTypes.RAW_DATA, NodeTypesVisual.BATCH_DATA, self._get_batch_data_exists(left_table_name),["show"],None),
            Node(conf_name, NodeTypes.JOIN, NodeTypesVisual.CONFIGURATION, True, ["backfill", "show-online-data"], config_file_path),
            Node(training_data_set_name, NodeTypes.BACKFILL_JOIN,NodeTypesVisual.BATCH_DATA, self._get_batch_data_exists(training_data_set_name),["show"],None),
        ]
        for node in nodes:
            self._add_node_once(node)

        self._add_edge_once(Edge(
            source=left_table_name,
            target=conf_name,
            exists=True
        ))

        self._add_edge_once(Edge(
            source=conf_name,
            target=training_data_set_name,
            exists=False
        ))

        for join_part in join_parts:
            group_by_name = join_part["groupBy"]["metaData"]["name"]
            edge = Edge(
                source=group_by_name,
                target=conf_name,
                exists=True
            )
            self._add_edge_once(edge)

    def _get_short_config_file_path(self, config_file_path: str) -> str:
        """/app/server/chronon_config/compiled/group_bys/quickstart/users.v1__1 
        -> compiled/group_bys/quickstart/users.v1__1"""
        match = re.search(r'(compiled/.*)$', config_file_path)
        if match:
            return match.group(1)
        return config_file_path

    def _add_orphan_raw_data_nodes(self) -> None:
        if self._datascanner is None:
            return
        all_raw_data_nodes = self._datascanner.list_tables(db_name=HARDCODED_DB_NAME, with_db_name=True)
        raw_data_nodes_from_conf = [x.name for x in self.graph.nodes if x.type  == NodeTypes.RAW_DATA.value]
        
        for raw_data_node in all_raw_data_nodes:
            if raw_data_node not in raw_data_nodes_from_conf:
                self.graph.add_node(Node(raw_data_node, NodeTypes.RAW_DATA, NodeTypesVisual.BATCH_DATA, True, ["show"], None))

    def _add_orphan_streaming_data_nodes_FAKE(self) -> None:
        if self._datascanner is None:
            return
        all_streaming_data_nodes = ["events.page_views", "events.logins"]
        streaming_data_nodes_from_conf = [x.name for x in self.graph.nodes if x.type  == NodeTypes.EVENT_STREAM.value]

        for streaming_data_node in all_streaming_data_nodes:
            if streaming_data_node not in streaming_data_nodes_from_conf:
                self.graph.add_node(Node(streaming_data_node, NodeTypes.EVENT_STREAM, NodeTypesVisual.STREAMING_DATA, True, None, None))

    def _iter_compiled_files(self, directory_path: Optional[str]) -> Iterable[Tuple[str, str]]:
        """Yield (file_path, short_config_file_path) for compiled config files."""
        if not directory_path or not os.path.isdir(directory_path):
            return []

        for entry in sorted(os.listdir(directory_path)):
            file_path = os.path.join(directory_path, entry)
            if not os.path.isfile(file_path) or entry in self.IGNORE_FILES:
                continue

            yield file_path, self._get_short_config_file_path(file_path)

    def parse(self) -> Dict[str, Any]:

        for file_path, short_path in self._iter_compiled_files(self._directory_path_gbs):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    compiled_data = json.load(f)
                self._add_compiled_to_graph_gbs(compiled_data, short_path)
            except Exception as exc:  # noqa: BLE001
                logger.error("Skipping file %s: %s", file_path, exc)

        for file_path, short_path in self._iter_compiled_files(self._directory_path_joins):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    compiled_data = json.load(f)
                self._add_compiled_to_graph_joins(compiled_data, short_path)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Skipping file %s: %s", file_path, exc)

        self._add_orphan_raw_data_nodes()
        self._add_orphan_streaming_data_nodes_FAKE()
        return self.graph.to_dict()
