import json
import os
import re
import logging
from typing import Any, Dict, List
from server.services.datascanner import DataScanner

logger = logging.getLogger("uvicorn.error")
class Node:
    def __init__(self, name: str, node_type: str, type_visual: str, exists: bool, actions: List[str], config_file_path: str=None):
        self.name = name
        self.node_type = node_type
        self.type_visual = type_visual
        self.exists = exists
        self.actions = actions
        self.config_file_path = config_file_path

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.node_type,
            "type_visual": self.type_visual,
            "exists": self.exists,
            "actions": self.actions,
            "config_file_path": self.config_file_path,
        }


class Edge:
    def __init__(self, source: str, target: str, edge_type: str, exists: bool):
        self.source = source
        self.target = target
        self.edge_type = edge_type
        self.exists = exists

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "type": self.edge_type,
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
    IGNORE_FILES = ["schema.v1__1"]
    def __init__(self, directory_path_gbs: str, directory_path_joins: str = None,datascanner: DataScanner = None):
        self._directory_path_gbs = directory_path_gbs
        self._directory_path_joins = directory_path_joins
        self._datascanner = datascanner
        self.graph = Graph()

    def _get_batch_data_exists(self, table_name: str) -> bool:
        # If no datascanner is provided, we can't check existence
        if self._datascanner is None:
            return False
        # TODO: Get the database name from the table name
        database_name = table_name.split(".")[0]
        table_name = table_name.split(".")[1]
        return self._datascanner.get_table_exists(database_name, table_name)

    def _add_compiled_to_graph_gbs(self, compiled_data: Dict[str, Any], seen_nodes: set, seen_edges: set, config_file_path: str=None) -> None:
        conf_name: str = compiled_data["metaData"]["name"]
        raw_table_name: str = compiled_data["sources"][0]["events"]["table"]
        team_name: str = compiled_data["metaData"]["team"]
        backfill_name = f"{team_name}.{_underscore_name(conf_name)}"
        upload_name = f"{team_name}.{_underscore_name(conf_name)}__upload"

        nodes = [
            Node(conf_name, "conf-group_by", "conf", True, ["backfill", "upload"], config_file_path),
            Node(raw_table_name, "raw-data", "batch-data", self._get_batch_data_exists(raw_table_name), ["show"], None),
            Node(backfill_name, "backfill-group_by", "batch-data", self._get_batch_data_exists(backfill_name), ["show"], None),
            Node(upload_name, "upload-group_by", "batch-data", self._get_batch_data_exists(upload_name), ["show"], None),
        ]
        for n in nodes:
            if n.name not in seen_nodes:
                self.graph.add_node(n)
                seen_nodes.add(n.name)

        edges = [
            Edge(raw_table_name, conf_name, "raw-data-to-conf", True),
            Edge(conf_name, backfill_name, "conf-to-backfill-group_by", True),
            Edge(conf_name, upload_name, "conf-to-upload-group_by", True),
        ]
        for e in edges:
            key = (e.source, e.target, e.edge_type)
            if key not in seen_edges:
                self.graph.add_edge(e)
                seen_edges.add(key)

    def _add_compiled_to_graph_joins(self, compiled_data: Dict[str, Any], config_file_path: str=None) -> None:
        print("Adding Join compiled data: %s", config_file_path)
        conf_name: str = compiled_data["metaData"]["name"]
        join_parts = compiled_data["joinParts"]
        training_data_set_name = f"training_data.{conf_name}"
        left_table_name = compiled_data["left"]["events"]["table"]
        print("Adding Nodes: %s", conf_name)
        nodes = [
            Node(
                name=left_table_name,
                node_type="raw-data",
                type_visual="batch-data",
                exists=self._get_batch_data_exists(left_table_name),
                actions=["show"],
                config_file_path=None
            ),
            Node(conf_name, "conf-join", "conf", True, ["backfill"], config_file_path),
            Node(
                name=training_data_set_name,node_type="backfill-join",
                type_visual="batch-data",
                exists=False,
                actions=["show"],
                config_file_path=None
            ),
        ]
        for n in nodes:
            print("Adding Join node: %s", n.name)
            self.graph.add_node(n)

        self.graph.add_edge(Edge(
            source=left_table_name,
            target=conf_name,
            edge_type="raw-data-to-conf",
            exists="True"
        ))

        self.graph.add_edge(Edge(
            source=conf_name,
            target=training_data_set_name,
            edge_type="conf-to-training-data-set",
            exists="False"
        ))

        for join_part in join_parts:
            group_by_name = join_part["groupBy"]["metaData"]["name"]
            edge = Edge(
                source=group_by_name,
                target=conf_name,
                edge_type="conf-to-conf",
                exists="True"
            )
            self.graph.add_edge(edge)

    def _get_short_config_file_path(self, config_file_path: str) -> str:
        """/app/server/chronon_config/compiled/group_bys/quickstart/users.v1__1 
        -> compiled/group_bys/quickstart/users.v1__1"""
        match = re.search(r'(compiled/.*)$', config_file_path)
        if match:
            return match.group(1)
        return config_file_path

    def parse(self) -> Dict[str, Any]:
        # Directory of compiled files
        if self._directory_path_gbs and os.path.isdir(self._directory_path_gbs):
            print("Parsing GroupBys compiled directory: %s", self._directory_path_gbs)
            seen_nodes: set = set()
            seen_edges: set = set()

            for entry in sorted(os.listdir(self._directory_path_gbs)):
                file_path = os.path.join(self._directory_path_gbs, entry)
                short_config_file_path = self._get_short_config_file_path(file_path)
                if not os.path.isfile(file_path) or entry in self.IGNORE_FILES:
                    continue
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        compiled_data = json.load(f)
                    self._add_compiled_to_graph_gbs(compiled_data, seen_nodes, seen_edges, short_config_file_path)
                except Exception as exc:
                    logger.debug("Skipping file %s: %s", file_path, exc)
                    continue

        if self._directory_path_joins and os.path.isdir(self._directory_path_joins):
            print("Parsing Joins compiled directory: %s", self._directory_path_joins)

            for entry in sorted(os.listdir(self._directory_path_joins)):
                print("Entry: %s", entry)
                file_path = os.path.join(self._directory_path_joins, entry)
                print("File path: %s", file_path)
                short_config_file_path = self._get_short_config_file_path(file_path)
                print("Short config file path: %s", short_config_file_path)
                if not os.path.isfile(file_path) or entry in self.IGNORE_FILES:
                    print("Skipping file123: %s", file_path)
                    continue
                    
                try:
                    print("Opening file: %s", file_path)
                    with open(file_path, "r", encoding="utf-8") as f:
                        compiled_data = json.load(f)
                    print("Short config file path: %s", short_config_file_path)
                    self._add_compiled_to_graph_joins(compiled_data,  short_config_file_path)
                except Exception as exc:
                    logger.debug("Skipping file %s: %s", file_path, exc)
                    continue
        


        return self.graph.to_dict()