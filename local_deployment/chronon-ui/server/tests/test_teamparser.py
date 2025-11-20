import pytest
from pathlib import Path
from server.services.teamparser import TeamParser


@pytest.fixture
def test_metadata_path():
    """Return the path to the test teams_metadata directory."""
    current_dir = Path(__file__).parent
    return str(current_dir / "test_data" / "teams_metadata")


def test_parse_teams_metadata(test_metadata_path):
    """Test parsing team metadata and verify expected structure."""
    parser = TeamParser(test_metadata_path)
    teams = parser.parse()

    # Verify the complete structure matches expected
    assert teams == {
        "default": {
            "executionInfo": {
                "conf": {
                    "common": {
                        "spark.chronon.coalesce.factor": "10",
                        "spark.chronon.partition.column": "<partition-column-name>",
                        "spark.chronon.partition.format": "<date-format>",
                    }
                },
                "env": {
                    "common": {
                        "ARTIFACT_PREFIX": "<customer-artifact-bucket>",
                        "CLOUD_PROVIDER": "aws",
                        "CUSTOMER_ID": "<customer_id>",
                    }
                },
            }
        },
        "quickstart": {
            "executionInfo": {
                "conf": {"common": {"spark.chronon.coalesce.factor": "10"}},
                "env": {"common": {"ARTIFACT_PREFIX": "<customer-artifact-bucket>"}},
            }
        },
    }


def test_parse_team_names_only(test_metadata_path):
    """Test parsing with team_names_only=True returns only team names."""
    parser = TeamParser(test_metadata_path)
    team_names = parser.parse(team_names_only=True)

    # Should return a sorted list of team names
    assert isinstance(team_names, list)
    assert team_names == ["default", "quickstart"]
