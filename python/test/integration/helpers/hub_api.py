"""Direct HTTP helpers for Hub API endpoints not exposed via CLI."""

import os

import requests


def _get_auth_headers() -> dict:
    headers = {}
    token = os.environ.get("GCP_ID_TOKEN") or os.environ.get("AWS_ID_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def list_schedules(hub_url: str) -> list[dict]:
    """GET /schedule/v2/schedules — return all schedules."""
    resp = requests.get(f"{hub_url}/schedule/v2/schedules", headers=_get_auth_headers())
    resp.raise_for_status()
    return resp.json().get("schedules", [])


def delete_schedule(hub_url: str, conf_name: str) -> dict:
    """DELETE /schedule/v2/schedules?confName=... — delete all schedules for a conf."""
    resp = requests.delete(
        f"{hub_url}/schedule/v2/schedules",
        params={"confName": conf_name},
        headers=_get_auth_headers(),
    )
    resp.raise_for_status()
    return resp.json()


def find_schedules_by_test_id(hub_url: str, test_id: str) -> list[dict]:
    """List schedules and filter to those whose confName contains *test_id*."""
    return [s for s in list_schedules(hub_url) if test_id in s.get("confName", "")]
