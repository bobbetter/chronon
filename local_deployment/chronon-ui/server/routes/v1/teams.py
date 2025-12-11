import os
import logging
from typing import Dict, List, Any
from fastapi import APIRouter, Request, Depends, HTTPException
from server.services.teamparser import TeamParser
from server.errors import TeamNotFoundError
path = os.path.basename(os.path.dirname(__file__))
router = APIRouter(prefix=f"/{path}/teams", tags=["teams"])
logger = logging.getLogger("uvicorn.error")


def get_teamparser(request: Request) -> TeamParser:
    """Dependency to access the singleton TeamParser from app state."""
    return request.app.state.teamparser


@router.get("")
def list_teams(
    parser: TeamParser = Depends(get_teamparser),
) -> Dict[str, Any] | List[str]:
    """
    Get list of all team names.

    Returns:
        Sorted list of team names: ["default", "quickstart"]
    """
    return parser.get_team_names()

@router.get("/metadata/{team_name}")
def get_team_metadata(
    team_name: str,
    parser: TeamParser = Depends(get_teamparser),
) -> Dict[str, Any]:
    """
    Get metadata for a specific team.
    """
    try:
        return parser.get_metadata(team_name)
    except TeamNotFoundError as e:
        logger.error(f"Team not found: {e}")
        raise HTTPException(
            status_code=404, detail=f"{str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error getting team metadata: {e}")
        raise HTTPException(
            status_code=500, detail=f"Unexpected error getting team metadata: {str(e)}"
        )