"""Integration tests for ``zipline hub run-adhoc`` and ``zipline hub cancel``."""

import pytest
from click.testing import CliRunner

from .helpers.cli import cancel_workflow, compile_configs, submit_run_adhoc
from .helpers.workflow import poll_workflow_until


@pytest.mark.integration
def test_gcp_run_adhoc_and_cancel(confs, chronon_root, hub_url):
    """run-adhoc launches a streaming deploy; cancel should terminate it."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(runner, chronon_root, hub_url, confs["compiled/joins/gcp/demo.v1__1"], "2025-08-01")

    # The deploy workflow succeeds once the streaming node is up.
    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"SUCCEEDED"}, timeout=300, interval=15
    )

    # Cancel the streaming workflow.
    cancel_workflow(runner, chronon_root, hub_url, workflow_id)

    # Verify the workflow reaches CANCELLED.
    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"CANCELLED"}, timeout=300, interval=15
    )


@pytest.mark.integration
def test_gcp_run_adhoc_no_data(confs, chronon_root, hub_url):
    """run-adhoc with dates that have no input data should fail."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(runner, chronon_root, hub_url, confs["compiled/joins/gcp/demo.v1__1"], "1969-01-01")

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"FAILED"}, timeout=900, interval=30
    )
