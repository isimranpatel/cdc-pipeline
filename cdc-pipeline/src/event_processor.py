"""
event_processor.py
------------------
Validates, transforms, and writes CDC events to the analytics store.
Handles schema drift detection and deduplication.
"""

import hashlib
import json
import logging
import os
import psycopg2
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger("cdc.processor")

ANALYTICS_DB = {
    "host": os.getenv("ANALYTICS_DB_HOST", "localhost"),
    "port": int(os.getenv("ANALYTICS_DB_PORT", 5433)),
    "dbname": os.getenv("ANALYTICS_DB_NAME", "analyticsdb"),
    "user": os.getenv("ANALYTICS_DB_USER", "postgres"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "postgres"),
}

# Expected schemas per table — used for drift detection
EXPECTED_SCHEMAS = {
    "orders": {"id", "customer_id", "product", "quantity", "amount", "status", "created_at", "updated_at"},
    "customers": {"id", "name", "email", "region", "created_at"},
}


class EventProcessor:
    def __init__(self):
        self.conn = self._connect()
        self.seen_events: set = set()  # In-memory deduplication cache

    def _connect(self):
        try:
            conn = psycopg2.connect(**ANALYTICS_DB)
            conn.autocommit = False
            logger.info("Connected to analytics database")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to analytics DB: {e}")
            raise

    def handle_insert(self, table: str, row: Dict[str, Any]):
        if not self._validate_schema(table, row):
            return
        if self._is_duplicate(table, row):
            return
        self._upsert(table, row, operation="INSERT")
        logger.debug(f"INSERT → {table}: id={row.get('id')}")

    def handle_update(self, table: str, before: Dict[str, Any], after: Dict[str, Any]):
        if not self._validate_schema(table, after):
            return
        self._detect_schema_drift(table, before, after)
        self._upsert(table, after, operation="UPDATE")
        logger.debug(f"UPDATE → {table}: id={after.get('id')}")

    def handle_delete(self, table: str, row: Dict[str, Any]):
        self._soft_delete(table, row)
        logger.debug(f"DELETE → {table}: id={row.get('id')}")

    def handle_snapshot(self, table: str, row: Dict[str, Any]):
        """Initial snapshot — bulk load without triggering downstream alerts."""
        if not self._validate_schema(table, row):
            return
        self._upsert(table, row, operation="SNAPSHOT")

    def _validate_schema(self, table: str, row: Dict[str, Any]) -> bool:
        """Detect schema drift — alert if unexpected columns appear or required columns missing."""
        expected = EXPECTED_SCHEMAS.get(table, set())
        actual = set(row.keys())
        missing = expected - actual
        unexpected = actual - expected

        if missing:
            logger.error(f"SCHEMA DRIFT [{table}] — missing columns: {missing}")
            return False
        if unexpected:
            logger.warning(f"SCHEMA DRIFT [{table}] — unexpected columns: {unexpected} (may need migration)")

        return True

    def _detect_schema_drift(self, table: str, before: Dict, after: Dict):
        """Compare before/after to detect column type changes."""
        for key in before:
            if key in after:
                before_type = type(before[key]).__name__
                after_type = type(after[key]).__name__
                if before_type != after_type:
                    logger.warning(
                        f"TYPE CHANGE [{table}.{key}]: {before_type} → {after_type}"
                    )

    def _is_duplicate(self, table: str, row: Dict[str, Any]) -> bool:
        """Deduplicate using content hash — prevents double-processing on consumer restart."""
        event_hash = hashlib.md5(
            f"{table}:{row.get('id')}:{row.get('updated_at', row.get('created_at'))}".encode()
        ).hexdigest()
        if event_hash in self.seen_events:
            logger.debug(f"Duplicate event skipped: {table} id={row.get('id')}")
            return True
        self.seen_events.add(event_hash)
        # Keep cache bounded
        if len(self.seen_events) > 100_000:
            self.seen_events.clear()
        return False

    def _upsert(self, table: str, row: Dict[str, Any], operation: str):
        """Upsert row into analytics store with CDC metadata."""
        if not row:
            return
        try:
            row_with_meta = {
                **row,
                "_cdc_operation": operation,
                "_cdc_processed_at": datetime.utcnow().isoformat(),
            }
            columns = list(row.keys())
            values = [row[c] for c in columns]
            placeholders = ", ".join(["%s"] * len(columns))
            col_str = ", ".join(columns)
            update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != "id"])

            sql = f"""
                INSERT INTO {table} ({col_str})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET {update_str}
            """
            with self.conn.cursor() as cur:
                cur.execute(sql, values)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Upsert failed [{table}]: {e}")
            raise

    def _soft_delete(self, table: str, row: Dict[str, Any]):
        """Mark row as deleted rather than hard delete — preserves audit trail."""
        try:
            sql = f"""
                UPDATE {table}
                SET _cdc_deleted = TRUE, _cdc_deleted_at = %s
                WHERE id = %s
            """
            with self.conn.cursor() as cur:
                cur.execute(sql, (datetime.utcnow().isoformat(), row.get("id")))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Soft delete failed [{table}]: {e}")
            raise
