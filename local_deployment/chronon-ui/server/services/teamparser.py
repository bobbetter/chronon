import json
import os
import logging
from typing import Any, Dict

logger = logging.getLogger("uvicorn.error")


class TeamParser:
    """Parses team metadata folders and files.
    
    Given a top-level metadata directory, this parser will:
    1. Find all team subdirectories
    2. Read the metadata file for each team
    3. Return a dictionary mapping team names to their metadata
    """
    
    def __init__(self, metadata_directory: str):
        """Initialize the TeamParser.
        
        Args:
            metadata_directory: Path to the top-level teams_metadata directory
        """
        self._metadata_directory = metadata_directory
    
    def parse(self, team_names_only: bool = False) -> Dict[str, Any] | list[str]:
        """Parse all team metadata files.
        
        Args:
            team_names_only: If True, return only team names as a list.
                           If False, return full metadata dictionary.
        
        Returns:
            If team_names_only is True:
                List of team names: ["default", "quickstart"]
            If team_names_only is False:
                Dictionary mapping team names to their metadata:
                {
                    "default": {...metadata...},
                    "quickstart": {...metadata...}
                }
        """
        teams_metadata = {}
        
        # Check if directory exists
        if not os.path.isdir(self._metadata_directory):
            logger.warning(
                "Metadata directory does not exist: %s", 
                self._metadata_directory
            )
            return [] if team_names_only else teams_metadata
        
        logger.info("Parsing teams metadata directory: %s", self._metadata_directory)
        
        # Iterate through subdirectories (each is a team)
        for entry in sorted(os.listdir(self._metadata_directory)):
            team_path = os.path.join(self._metadata_directory, entry)
            
            # Skip if not a directory
            if not os.path.isdir(team_path):
                continue
            
            team_name = entry
            metadata_file_name = f"{team_name}_team_metadata"
            metadata_file_path = os.path.join(team_path, metadata_file_name)
            
            # Check if metadata file exists
            if not os.path.isfile(metadata_file_path):
                logger.warning(
                    "Metadata file not found for team '%s': %s",
                    team_name,
                    metadata_file_path
                )
                continue
            
            # Read and parse the metadata file
            try:
                with open(metadata_file_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                teams_metadata[team_name] = metadata
                logger.info("Successfully parsed metadata for team: %s", team_name)
            except json.JSONDecodeError as exc:
                logger.error(
                    "Failed to parse JSON for team '%s' at %s: %s",
                    team_name,
                    metadata_file_path,
                    exc
                )
            except Exception as exc:
                logger.error(
                    "Error reading metadata for team '%s' at %s: %s",
                    team_name,
                    metadata_file_path,
                    exc
                )
        
        logger.info("Parsed %d team(s)", len(teams_metadata))
        
        if team_names_only:
            return sorted(teams_metadata.keys())
        return teams_metadata

