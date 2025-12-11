import json
import os
import logging
from typing import Any, Dict, List
from server.errors import TeamNotFoundError

logger = logging.getLogger("uvicorn.error")


class TeamParser:
    """Parses team metadata folders and files.

    Given a top-level metadata directory, this parser will:
    1. Find all team subdirectories
    2. Read the metadata file for each team
    3. Store the parsed metadata for retrieval
    """

    def __init__(self, metadata_directory: str):
        """Initialize the TeamParser and parse all team metadata.

        Args:
            metadata_directory: Path to the top-level teams_metadata directory
        """
        self._metadata_directory = metadata_directory
        self._teams_metadata: Dict[str, Any] = {}
        

    def _parse(self) -> None:
        """Parse all team metadata files and store in instance variable."""
        # Check if directory exists
        if not os.path.isdir(self._metadata_directory):
            logger.warning(
                "Metadata directory does not exist: %s", self._metadata_directory
            )
            return

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
                    metadata_file_path,
                )
                continue

            # Read and parse the metadata file
            try:
                with open(metadata_file_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                self._teams_metadata[team_name] = metadata
                logger.info("Successfully parsed metadata for team: %s", team_name)
            except json.JSONDecodeError as exc:
                logger.error(
                    "Failed to parse JSON for team '%s' at %s: %s",
                    team_name,
                    metadata_file_path,
                    exc,
                )
            except Exception as exc:
                logger.error(
                    "Error reading metadata for team '%s' at %s: %s",
                    team_name,
                    metadata_file_path,
                    exc,
                )

        logger.info("Parsed %d team(s)", len(self._teams_metadata))

    def get_team_names(self) -> List[str]:
        """Get list of all team names.

        Returns:
            Sorted list of team names: ["default", "quickstart"]
        """
        self._parse()
        return sorted(self._teams_metadata.keys())

    def get_metadata(self, team_name: str | None = None) -> Dict[str, Any] | None:
        """Get team metadata.

        Args:
            team_name: Optional team name to get metadata for a specific team.
                      If None, returns all team metadata.

        Returns:
            If team_name is provided:
                The metadata dict for that team, or None if not found.
            If team_name is None:
                Dictionary mapping team names to their metadata:
                {
                    "default": {...metadata...},
                    "quickstart": {...metadata...}
                }
        """
        self._parse()
        if team_name is not None:
            if team_name not in self._teams_metadata:
                raise TeamNotFoundError(f"Team not found: {team_name}")
            return self._teams_metadata[team_name]
        return self._teams_metadata
