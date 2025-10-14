import json
import os
import logging
from typing import Any, Dict, List

logger = logging.getLogger("uvicorn.error")
class Node:
    def __init__(self, name: str, node_type: str, type_visual: str, exists: bool, actions: List[str]):
        self.name = name
        self.node_type = node_type
        self.type_visual = type_visual
        self.exists = exists
        self.actions = actions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.node_type,
            "type_visual": self.type_visual,
            "exists": self.exists,
            "actions": self.actions,
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
    def __init__(self, source: Any):
        # Backward compatible: if dict provided, treat as single compiled object
        # If string path to directory is provided, parse all files within
        self._compiled_data = source if isinstance(source, dict) else None
        self._directory_path = source if isinstance(source, str) else None

    def _add_compiled_to_graph(self, compiled_data: Dict[str, Any], graph: Graph, seen_nodes: set, seen_edges: set) -> None:
        conf_name: str = compiled_data["metaData"]["name"]
        raw_table_name: str = compiled_data["sources"][0]["events"]["table"]

        # Nodes (preserve first-in order; dedupe by name)
        nodes = [
            Node(conf_name, "conf-group_by", "conf", True, ["backfill", "upload"]),
            Node(raw_table_name, "raw-data", "batch-data", True, ["show"]),
            Node(_underscore_name(conf_name), "backfill-group_by", "batch-data", False, ["show"]),
            Node(f"{_underscore_name(conf_name)}__upload", "upload-group_by", "batch-data", False, ["show"]),
        ]
        for n in nodes:
            if n.name not in seen_nodes:
                graph.add_node(n)
                seen_nodes.add(n.name)

        # Edges (dedupe by tuple key)
        backfill_name = _underscore_name(conf_name)
        upload_name = f"{backfill_name}__upload"
        edges = [
            Edge(raw_table_name, conf_name, "raw-data-to-conf", True),
            Edge(conf_name, backfill_name, "conf-to-backfill-group_by", False),
            Edge(conf_name, upload_name, "conf-to-upload-group_by", False),
        ]
        for e in edges:
            key = (e.source, e.target, e.edge_type)
            if key not in seen_edges:
                graph.add_edge(e)
                seen_edges.add(key)

    def parse(self) -> Dict[str, Any]:
        # Single compiled dict
        if self._compiled_data is not None:
            graph = Graph()
            logger.debug("Parsing single compiled data")
            self._add_compiled_to_graph(self._compiled_data, graph, set(), set())
            return graph.to_dict()

        # Directory of compiled files
        if self._directory_path and os.path.isdir(self._directory_path):
            graph = Graph()
            logger.info("Parsing compiled directory: %s", self._directory_path)
            seen_nodes: set = set()
            seen_edges: set = set()

            for entry in sorted(os.listdir(self._directory_path)):
                file_path = os.path.join(self._directory_path, entry)
                if not os.path.isfile(file_path):
                    continue
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        compiled_data = json.load(f)
                    self._add_compiled_to_graph(compiled_data, graph, seen_nodes, seen_edges)
                except Exception as exc:
                    # Skip unreadable/non-JSON files silently, but emit debug info
                    logger.debug("Skipping file %s: %s", file_path, exc)
                    continue

            return graph.to_dict()

        # If input type is unsupported, return empty graph
        return Graph().to_dict()