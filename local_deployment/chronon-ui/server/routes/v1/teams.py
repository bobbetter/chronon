import os
import logging
from typing import Dict, List, Any
from fastapi import APIRouter, Query, Request, Depends
from server.services.teamparser import TeamParser

path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/teams", tags=["teams"])
logger = logging.getLogger("uvicorn.error")


def get_teamparser(request: Request) -> TeamParser:
    """Dependency to access the singleton TeamParser from app state."""
    return request.app.state.teamparser


@router.get("/list")
def list_teams(
    names_only: bool = Query(False, description="If true, return only team names as a list"),
    parser: TeamParser = Depends(get_teamparser)
) -> Dict[str, Any] | List[str]:
    """
    Get teams metadata or just team names.
    
    Args:
        names_only: If True, return only team names as a list ["default", "quickstart"].
                   If False, return full metadata dictionary.
    
    Returns:
        Either a list of team names or a dictionary mapping team names to their metadata.
    """
    teams_data = parser.parse(team_names_only=names_only)
    
    if names_only:
        logger.info("Retrieved %d team name(s)", len(teams_data))
    else:
        logger.info("Retrieved metadata for %d team(s)", len(teams_data))
    
    return teams_data

