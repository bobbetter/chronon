"""Shared CLI helpers for integration tests."""

import json
import os
import shutil

from ai.chronon.repo.zipline import zipline


def compile_configs(runner, chronon_root, clean=True):
    """Compile canary configs, optionally from a clean state."""
    if clean:
        compiled_dir = os.path.join(chronon_root, "compiled")
        if os.path.exists(compiled_dir):
            shutil.rmtree(compiled_dir)

    result = runner.invoke(
        zipline,
        ["compile", f"--chronon-root={chronon_root}", "--force"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"compile failed:\n{result.output}"


def _extract_workflow_id(output: str) -> str:
    """Extract the workflowId from CLI JSON output."""
    json_start = output.rfind("{")
    assert json_start != -1, f"No JSON in output:\n{output}"
    response = json.loads(output[json_start:])
    workflow_id = response.get("workflowId")
    assert workflow_id, f"No workflowId in response:\n{output}"
    return workflow_id


def submit_backfill(runner, chronon_root, hub_url, conf, start_ds, end_ds):
    """Submit a backfill and return the workflow ID."""
    result = runner.invoke(
        zipline,
        [
            "hub", "backfill",
            f"--repo={chronon_root}",
            f"--conf={conf}",
            f"--hub-url={hub_url}",
            f"--start-ds={start_ds}",
            f"--end-ds={end_ds}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"backfill failed:\n{result.output}"
    return _extract_workflow_id(result.output)


def submit_run_adhoc(runner, chronon_root, hub_url, conf, end_ds):
    """Submit a run-adhoc deploy and return the workflow ID."""
    result = runner.invoke(
        zipline,
        [
            "hub", "run-adhoc",
            f"--repo={chronon_root}",
            f"--conf={conf}",
            f"--hub-url={hub_url}",
            f"--end-ds={end_ds}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"run-adhoc failed:\n{result.output}"
    return _extract_workflow_id(result.output)


def cancel_workflow(runner, chronon_root, hub_url, workflow_id):
    """Cancel a workflow via the CLI."""
    result = runner.invoke(
        zipline,
        [
            "hub", "cancel",
            f"--repo={chronon_root}",
            f"--hub-url={hub_url}",
            f"--workflow-id={workflow_id}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"cancel failed:\n{result.output}"


def submit_schedule(runner, chronon_root, hub_url, conf):
    """Deploy a recurring schedule for a conf."""
    result = runner.invoke(
        zipline,
        [
            "hub", "schedule",
            f"--repo={chronon_root}",
            f"--conf={conf}",
            f"--hub-url={hub_url}",
            "--format=json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"schedule failed:\n{result.output}"
