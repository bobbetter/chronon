import os
import logging
from fastapi import APIRouter
from services.graphparser import GraphParser

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/graph", tags=["graph"])

# Prefer the app logger (initialized in main.py); fallback to uvicorn.error
logger = logging.getLogger("uvicorn.error")

sample_graph = {
  "nodes": [
    {
      "name": "quickstart.page_views.v1__1",
      "type": "conf-group_by",
      "type_visual": "conf",
      "exists": True,
      "actions": ["backfill", "upload"]
    },
    {
      "name": "data.page_views",
      "type": "raw-data",
      "type_visual": "batch-data",
      "exists": True,
      "actions": ["show"]
    },
    {
      "name": "quickstart_page_views_v1__1",
      "type": "backfill-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"]
    },
    {
      "name": "quickstart_page_views_v1__1__upload",
      "type": "upload-group_by",
      "type_visual": "batch-data",
      "exists": False,
      "actions": ["show"]
    },
  ],
  "edges": [
    {
      "source": "data.page_views",
      "target": "quickstart.page_views.v1__1",
      "type": "raw-data-to-conf",
      "exists": True
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart_page_views_v1__1",
      "type": "conf-to-backfill-group_by",
      "exists": False
    },
    {
      "source": "quickstart.page_views.v1__1",
      "target": "quickstart_page_views_v1__1__upload",
      "type": "conf-to-upload-group_by",
      "exists": False
    },
  ]
}


@router.get("/graph_data")
def get_graph():
    current_dir = os.path.dirname(__file__)
    server_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    compiled_dir = os.path.join(server_root, "compiled", "group_bys", "quickstart")
    parser = GraphParser(compiled_dir)
    graph_dict = parser.parse()
    try:
        num_nodes = len(graph_dict.get("nodes", []))
        num_edges = len(graph_dict.get("edges", []))
        logger.info("Parsed graph with %d nodes and %d edges", num_nodes, num_edges)
    except Exception:
        # Best-effort logging; never fail the request due to logging
        logger.debug("Graph parsed; unable to compute counts for logging")
    return graph_dict