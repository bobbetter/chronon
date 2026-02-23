"""Integration test replicating run_aws_quickstart.sh.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow


@pytest.mark.integration
def test_aws_backfill(confs, chronon_root, hub_url):
    """Compile canary configs from scratch, submit a backfill, and poll until success."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(runner, chronon_root, hub_url, confs["compiled/joins/aws/demo.derivations_v1__2"], "2025-08-01", "2025-08-01")

    poll_workflow(hub_url, workflow_id, timeout=900, interval=30)


@pytest.mark.integration
def test_aws_backfill_no_data(confs, chronon_root, hub_url):
    """Backfill with dates that have no input data should result in a failed workflow."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(runner, chronon_root, hub_url, confs["compiled/joins/aws/demo.derivations_v1__2"], "1969-01-01", "1969-01-01")

    with pytest.raises(RuntimeError, match="ended with status FAILED"):
        poll_workflow(hub_url, workflow_id, timeout=900, interval=30)
