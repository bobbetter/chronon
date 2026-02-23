"""Integration tests for the schedule lifecycle: deploy, verify, delete."""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_schedule
from .helpers.hub_api import delete_schedule, find_schedules_by_test_id


@pytest.mark.integration
def test_gcp_schedule_lifecycle(confs, test_id, chronon_root, hub_url):
    """Deploy a schedule, verify it exists, delete it, verify it's gone."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    # Deploy the schedule.
    submit_schedule(runner, chronon_root, hub_url, confs["compiled/joins/gcp/demo.v1__1"])

    # Verify the schedule appears in the list.
    schedules = find_schedules_by_test_id(hub_url, test_id)
    assert len(schedules) == 2, f"No schedules found for test_id={test_id}"
    conf_name = schedules[0]["confName"]

    # Delete the schedule.
    delete_schedule(hub_url, conf_name)

    # Verify the schedule is gone.
    schedules = find_schedules_by_test_id(hub_url, test_id)
    assert len(schedules) == 0, f"Schedules still present after delete: {schedules}"
