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
    teams = parser.get_metadata()

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

    team = parser.get_metadata("quickstart")

    assert team == {
        "executionInfo": {
            "conf": {"common": {"spark.chronon.coalesce.factor": "10"}},
            "env": {"common": {"ARTIFACT_PREFIX": "<customer-artifact-bucket>"}},
        }
    }


def test_get_team_names(test_metadata_path):
    """Test get_team_names returns only team names."""
    parser = TeamParser(test_metadata_path)
    team_names = parser.get_team_names()

    # Should return a sorted list of team names
    assert isinstance(team_names, list)
    assert team_names == ["default", "quickstart"]
