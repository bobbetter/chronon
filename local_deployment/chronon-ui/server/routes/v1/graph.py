import os
import logging
from pathlib import Path
from fastapi import APIRouter
from server.services.graphparser import GraphParser
from server.services.datascanner import DataScanner
from fastapi import Depends
from server.routes.v1.spark_data import get_datascanner

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/graph", tags=["graph"])
logger = logging.getLogger("uvicorn.error")

current_dir = Path(__file__).parent
server_root = current_dir.parent.parent
compiled_dir_gbs = server_root / "chronon_config" / "compiled" / "group_bys" / "quickstart"
compiled_dir_joins = server_root / "chronon_config" / "compiled" / "joins" / "quickstart"
@router.get("/graph_data")
def get_graph(scanner: DataScanner = Depends(get_datascanner)):
    parser = GraphParser(
        str(compiled_dir_gbs), 
        compiled_dir_joins,
        scanner)
    graph_dict = parser.parse()
    try:
        num_nodes = len(graph_dict.get("nodes", []))
        num_edges = len(graph_dict.get("edges", []))
        logger.info("Parsed graph with %d nodes and %d edges", num_nodes, num_edges)
    except Exception:
        # Best-effort logging; never fail the request due to logging
        logger.debug("Graph parsed; unable to compute counts for logging")
    return graph_dict