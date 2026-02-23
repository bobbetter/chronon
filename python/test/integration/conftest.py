"""Fixtures for Hub/Orchestrator integration tests.

Usage::

    # Run all integration tests:
    GCP_ID_TOKEN=$(gcloud auth print-identity-token) \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration

    # Run a specific test:
    PYTEST_ADDOPTS="-k test_gcp_backfill_no_data" \\
        GCP_ID_TOKEN=$(gcloud auth print-identity-token) \\
        HUB_URL=https://canary-orch.zipline.ai \\
        ./mill python.test_integration
"""

import logging
import os
import random
import string
import sys

import pytest

from .helpers.cleanup import AWSCleanup, GCPCleanup
from .helpers.templates import cleanup_test_configs, generate_test_configs, get_confs

logger = logging.getLogger(__name__)

def pytest_addoption(parser):
    parser.addoption(
        "--hub-url",
        default=os.environ.get("HUB_URL", "http://localhost:3903"),
        help="Hub base URL (env: HUB_URL)",
    )
    parser.addoption(
        "--cloud",
        default=os.environ.get("CLOUD", "gcp"),
        choices=("gcp", "aws"),
        help="Cloud provider (env: CLOUD)",
    )
    parser.addoption(
        "--environment",
        default=os.environ.get("ENVIRONMENT", "canary"),
        help="Deployment environment, e.g. canary / dev (env: ENVIRONMENT)",
    )


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def hub_url(request) -> str:
    return request.config.getoption("--hub-url")


@pytest.fixture(scope="session")
def cloud(request) -> str:
    return request.config.getoption("--cloud")


@pytest.fixture
def environment(request) -> str:
    """Per-test environment. Override with @pytest.mark.environment("dev")."""
    marker = request.node.get_closest_marker("environment")
    if marker:
        return marker.args[0]
    return request.config.getoption("--environment")


@pytest.fixture(scope="session")
def chronon_root() -> str:
    """Absolute path to the canary config root."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "canary")


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------

ARTIFACT_PREFIXES = {
    "gcp": "gs://zipline-artifacts-{environment}",
    "aws": "s3://zipline-artifacts-{environment}",
}


@pytest.fixture(autouse=True)
def chronon_env(monkeypatch, chronon_root, cloud, environment):
    """Set up the environment variables and sys.path needed by canary configs."""
    prefix = ARTIFACT_PREFIXES[cloud].format(environment=environment)
    monkeypatch.setenv("PYTHONPATH", chronon_root)
    monkeypatch.setenv("ARTIFACT_PREFIX", prefix)
    monkeypatch.setenv("CUSTOMER_ID", environment)
    monkeypatch.syspath_prepend(chronon_root)


def _random_suffix(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.fixture
def confs(test_id, cloud) -> dict[str, str]:
    """Compiled conf paths keyed by logical name, resolved with the current test_id."""
    return get_confs(cloud, test_id)


@pytest.fixture
def test_id(request, chronon_root, cloud, environment):
    """Generate a unique test_id, render templates, and clean up afterwards."""
    tid = _random_suffix()
    logger.info("test_id=%s for %s", tid, request.node.name)

    generate_test_configs(tid, chronon_root, cloud=cloud)

    yield tid

    # --- teardown ---
    cleanup_test_configs(tid, chronon_root, cloud=cloud)

    try:
        if cloud == "gcp":
            project = os.environ.get("GCP_BQ_PROJECT", "canary-443022")
            dataset = os.environ.get("GCP_BQ_DATASET", "data")
            GCPCleanup(project, dataset).cleanup_tables(tid)
        elif cloud == "aws":
            database = os.environ.get("AWS_GLUE_DATABASE", "default")
            AWSCleanup(database).cleanup_tables(tid)
    except Exception:
        logger.exception("Table cleanup failed for test_id=%s", tid)
