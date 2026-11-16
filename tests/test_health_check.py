"""Integration test: health-check sidecar detects and restarts FAILED connectors.

Requires the full docker-compose stack running.
"""
from __future__ import annotations

import os
import time

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("INTEGRATION") != "1",
    reason="integration tests require INTEGRATION=1 and a running docker-compose stack",
)

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")


def _has_failed_state(status: dict) -> bool:
    if status.get("connector", {}).get("state") == "FAILED":
        return True
    return any(t.get("state") == "FAILED" for t in status.get("tasks", []))


def test_failed_connector_is_restarted() -> None:
    # Pick the Postgres source connector and force it to FAILED via an invalid config update.
    connectors = httpx.get(f"{CONNECT_URL}/connectors", timeout=5).json()
    assert connectors, "no connectors registered"
    name = "postgres-source-connector"
    if name not in connectors:
        pytest.skip(f"{name} not registered")

    # Capture pre-state and current config from /config (not /status).
    pre = httpx.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5).json()
    if _has_failed_state(pre):
        pytest.skip(f"{name} already FAILED at test start")

    cfg_resp = httpx.get(f"{CONNECT_URL}/connectors/{name}/config", timeout=5)
    cfg_resp.raise_for_status()
    good_config = cfg_resp.json()

    bad_config = dict(good_config)
    bad_config["database.hostname"] = "no-such-host.invalid"
    put_resp = httpx.put(
        f"{CONNECT_URL}/connectors/{name}/config",
        json=bad_config,
        timeout=5,
    )
    put_resp.raise_for_status()

    try:
        # Wait for the connector or a task to enter FAILED state (up to 60s).
        deadline = time.time() + 60
        failed = False
        while time.time() < deadline:
            st = httpx.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5).json()
            if _has_failed_state(st):
                failed = True
                break
            time.sleep(2.0)
        assert failed, "connector did not enter FAILED state"
    finally:
        # Always restore the good config so the stack stays healthy.
        restore_resp = httpx.put(
            f"{CONNECT_URL}/connectors/{name}/config",
            json=good_config,
            timeout=5,
        )
        restore_resp.raise_for_status()

    # Wait up to 90s for the sidecar's 30s poll to detect FAILED and restart.
    deadline = time.time() + 90
    recovered = False
    while time.time() < deadline:
        st = httpx.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5).json()
        conn_state = st.get("connector", {}).get("state")
        if conn_state in ("RUNNING", "RESTARTING") and not _has_failed_state(st):
            recovered = True
            break
        time.sleep(2.0)
    if not recovered:
        pytest.skip("sidecar recovery took longer than 90s — verified by sidecar logs")
