import os
import logging
from fastapi import APIRouter, Depends, Request, HTTPException, Query
from server.services.graphparser import GraphParser
from server.services.confparser import ConfParser, ConfType

from server.errors import TeamNotFoundError
from server.config import compiled_dir_gbs, compiled_dir_joins, ComputeEngine

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/graph", tags=["graph"])
logger = logging.getLogger("uvicorn.error")


def get_graphparser(request: Request) -> GraphParser:
    """Dependency to access the singleton GraphParser from app state."""
    return request.app.state.graphparser


@router.get("/{team_name}/graph_data")
def get_graph(
    team_name: str,
    compute_engine: ComputeEngine = Query(
        default=ComputeEngine.LOCAL,
        description="Compute engine to use: 'local' for local Spark warehouse, 'remote' for AWS Glue",
    ),
    parser: GraphParser = Depends(get_graphparser),
):
    
    try:
        graph_dict = parser.parse(team_name, compute_engine)
        num_nodes = len(graph_dict.get("nodes", []))
        num_edges = len(graph_dict.get("edges", []))
        logger.info(
            "Parsed graph with %d nodes and %d edges (compute_engine=%s)",
            num_nodes,
            num_edges,
            compute_engine.value,
        )
        return graph_dict

    except Exception as e:
        logger.error(f"Unexpected error parsing graph: {e}")
        raise HTTPException(
            status_code=500, detail=f"Unexpected error parsing graph: {str(e)}"
        )


@router.get("/{team_name}/list_confs")
def list_confs(team_name: str, conf_type: ConfType):
    """List all configuration files for a given team and configuration type."""
    if conf_type == ConfType.GROUP_BY:
        directory_path = compiled_dir_gbs
    else:
        directory_path = compiled_dir_joins
    conf_parser = ConfParser(directory_path=directory_path)

    try:
        return conf_parser.parse(team_name)
    except TeamNotFoundError as e:
        logger.error(f"Team not found: {e}")
        raise HTTPException(
            status_code=404, detail=f"{str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error parsing confs: {e}")
        raise HTTPException(
            status_code=500, detail=f"Unexpected error parsing confs: {str(e)}"
        )
