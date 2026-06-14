"""
tests/test_lag_monitor.py
Unit tests for replication lag monitor.
"""

import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def monitor():
    with patch("lag_monitor.psycopg2.connect") as mock_conn:
        mock_conn.return_value = MagicMock()
        from lag_monitor import LagMonitor
        m = LagMonitor()
        m.conn = MagicMock()
        return m


def test_lag_alert_recorded(monitor):
    monitor.record_lag_alert(750)
    assert len(monitor.lag_alerts) == 1
    assert monitor.lag_alerts[0]["lag_ms"] == 750


def test_multiple_lag_alerts(monitor):
    monitor.record_lag_alert(600)
    monitor.record_lag_alert(800)
    monitor.record_lag_alert(1200)
    assert len(monitor.lag_alerts) == 3


def test_slot_status_ok(monitor):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("debezium_slot", True, "2MB", 2 * 1024 * 1024, None, None)
    ]
    monitor.conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    monitor.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    slots = monitor._check_slots()
    assert slots[0]["status"] == "OK"


def test_slot_bloat_detected(monitor):
    cursor = MagicMock()
    # Simulate 600MB retained (over 512MB threshold)
    cursor.fetchall.return_value = [
        ("debezium_slot", True, "600MB", 600 * 1024 * 1024, None, None)
    ]
    monitor.conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    monitor.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    slots = monitor._check_slots()
    assert "WARNING" in slots[0]["status"]


def test_inactive_slot_flagged(monitor):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("debezium_slot", False, "1GB", 1024 * 1024 * 1024, None, None)
    ]
    monitor.conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    monitor.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    slots = monitor._check_slots()
    assert "INACTIVE" in slots[0]["status"]
