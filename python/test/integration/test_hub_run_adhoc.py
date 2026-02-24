"""Cloud-agnostic integration tests for ``zipline hub run-adhoc`` and ``zipline hub cancel``.

Replaces the former test_gcp_run_adhoc.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import cancel_workflow, compile_configs, submit_run_adhoc
from .helpers.workflow import poll_workflow_until

DEMO_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/joins/azure/demo.v2",
}


@pytest.mark.integration
def test_run_adhoc_and_cancel(confs, chronon_root, hub_url, cloud):
    """run-adhoc launches a streaming deploy; cancel should terminate it."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs[DEMO_BACKFILL[cloud]], "2025-08-01",
    )

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"SUCCEEDED"}, timeout=300, interval=15,
    )

    cancel_workflow(runner, chronon_root, hub_url, workflow_id, cloud)

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"CANCELLED"}, timeout=300, interval=15,
    )


@pytest.mark.integration
def test_run_adhoc_no_data(confs, chronon_root, hub_url, cloud):
    """run-adhoc with dates that have no input data should fail."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs[DEMO_BACKFILL[cloud]], "1969-01-01",
    )

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"FAILED"}, timeout=900, interval=30,
    )
