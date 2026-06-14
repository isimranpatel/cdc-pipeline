"""
tests/test_event_processor.py
Unit tests for CDC event processor — schema validation, deduplication, drift detection.
"""

import pytest
from unittest.mock import MagicMock, patch


# ── Fixtures ────────────────────────────────────────────────

@pytest.fixture
def processor():
    with patch("event_processor.psycopg2.connect") as mock_conn:
        mock_conn.return_value = MagicMock()
        from event_processor import EventProcessor
        proc = EventProcessor()
        proc.conn = MagicMock()
        return proc


# ── Schema Validation ────────────────────────────────────────

def test_valid_order_schema(processor):
    row = {
        "id": 1, "customer_id": 2, "product": "Laptop",
        "quantity": 1, "amount": 999.99, "status": "pending",
        "created_at": "2024-01-01", "updated_at": "2024-01-01"
    }
    assert processor._validate_schema("orders", row) is True


def test_missing_required_column(processor):
    row = {"id": 1, "product": "Laptop"}  # missing many required fields
    result = processor._validate_schema("orders", row)
    assert result is False


def test_unexpected_column_still_passes(processor):
    row = {
        "id": 1, "customer_id": 2, "product": "Laptop",
        "quantity": 1, "amount": 999.99, "status": "pending",
        "created_at": "2024-01-01", "updated_at": "2024-01-01",
        "new_mystery_column": "surprise"  # unexpected but not blocking
    }
    # Should return True (warning only, not blocking)
    assert processor._validate_schema("orders", row) is True


# ── Deduplication ────────────────────────────────────────────

def test_duplicate_detection(processor):
    row = {"id": 42, "updated_at": "2024-01-01T12:00:00"}
    assert processor._is_duplicate("orders", row) is False  # first time
    assert processor._is_duplicate("orders", row) is True   # second time = duplicate


def test_different_rows_not_duplicate(processor):
    row_a = {"id": 1, "updated_at": "2024-01-01"}
    row_b = {"id": 2, "updated_at": "2024-01-01"}
    processor._is_duplicate("orders", row_a)
    assert processor._is_duplicate("orders", row_b) is False


def test_same_id_different_timestamp_not_duplicate(processor):
    row_a = {"id": 1, "updated_at": "2024-01-01T10:00:00"}
    row_b = {"id": 1, "updated_at": "2024-01-01T11:00:00"}  # updated
    processor._is_duplicate("orders", row_a)
    assert processor._is_duplicate("orders", row_b) is False


# ── Schema Drift Detection ────────────────────────────────────

def test_no_drift_same_types(processor, caplog):
    before = {"id": 1, "amount": 99.99}
    after  = {"id": 1, "amount": 149.99}
    processor._detect_schema_drift("orders", before, after)
    assert "TYPE CHANGE" not in caplog.text


def test_type_change_detected(processor, caplog):
    import logging
    caplog.set_level(logging.WARNING)
    before = {"id": 1, "amount": 99.99}   # float
    after  = {"id": 1, "amount": "99.99"} # now string — drift!
    processor._detect_schema_drift("orders", before, after)
    assert "TYPE CHANGE" in caplog.text
