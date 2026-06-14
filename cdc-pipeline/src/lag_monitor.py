"""
lag_monitor.py
--------------
Monitors replication lag, slot bloat, and consumer offset lag.
Produces a health report and logs alerts when thresholds are exceeded.
"""

import logging
import os
import psycopg2
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger("cdc.monitor")

SOURCE_DB = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", 5432)),
    "dbname": os.getenv("SOURCE_DB_NAME", "sourcedb"),
    "user": os.getenv("SOURCE_DB_USER", "postgres"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "postgres"),
}

THRESHOLDS = {
    "lag_ms": 500,            # Replication lag alert threshold (ms)
    "slot_retained_mb": 512,  # WAL slot bloat alert threshold (MB)
    "dlq_depth": 10,          # Dead letter queue depth alert
}


class LagMonitor:
    def __init__(self):
        self.lag_alerts = []
        self.conn = self._connect()

    def _connect(self):
        try:
            return psycopg2.connect(**SOURCE_DB)
        except psycopg2.Error as e:
            logger.error(f"Monitor failed to connect to source DB: {e}")
            return None

    def report(self) -> Dict[str, Any]:
        """Generate a full replication health report."""
        results = {
            "timestamp": datetime.utcnow().isoformat(),
            "replication_slots": self._check_slots(),
            "replication_lag": self._check_lag(),
            "active_connections": self._check_connections(),
        }
        self._print_report(results)
        return results

    def _check_slots(self):
        """Check replication slot health and WAL retention."""
        if not self.conn:
            return []
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        slot_name,
                        active,
                        restart_lsn,
                        confirmed_flush_lsn,
                        pg_size_pretty(
                            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                        ) AS retained_wal,
                        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
                    FROM pg_replication_slots
                    WHERE slot_type = 'logical';
                """)
                slots = []
                for row in cur.fetchall():
                    retained_mb = (row[5] or 0) / (1024 * 1024)
                    status = "OK"
                    if not row[1]:
                        status = "INACTIVE — slot not consuming WAL!"
                    elif retained_mb > THRESHOLDS["slot_retained_mb"]:
                        status = f"WARNING — slot bloat: {row[4]} retained"
                    slots.append({
                        "slot_name": row[0],
                        "active": row[1],
                        "retained_wal": row[4],
                        "retained_mb": round(retained_mb, 2),
                        "status": status,
                    })
                return slots
        except psycopg2.Error as e:
            logger.error(f"Slot check failed: {e}")
            return []

    def _check_lag(self):
        """Check current replication lag."""
        if not self.conn:
            return None
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        now() - pg_last_xact_replay_timestamp() AS lag,
                        EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000 AS lag_ms
                """)
                row = cur.fetchone()
                if row:
                    lag_ms = round(row[1] or 0, 2)
                    return {
                        "lag": str(row[0]),
                        "lag_ms": lag_ms,
                        "status": "OK" if lag_ms < THRESHOLDS["lag_ms"]
                                  else f"WARNING — lag {lag_ms}ms exceeds threshold {THRESHOLDS['lag_ms']}ms",
                    }
        except psycopg2.Error as e:
            logger.error(f"Lag check failed: {e}")
        return None

    def _check_connections(self):
        """List active replication connections."""
        if not self.conn:
            return []
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT pid, usename, application_name, state, sent_lsn, write_lsn
                    FROM pg_stat_replication;
                """)
                return [
                    {
                        "pid": row[0],
                        "user": row[1],
                        "app": row[2],
                        "state": row[3],
                        "sent_lsn": str(row[4]),
                        "write_lsn": str(row[5]),
                    }
                    for row in cur.fetchall()
                ]
        except psycopg2.Error as e:
            logger.error(f"Connection check failed: {e}")
            return []

    def record_lag_alert(self, lag_ms: int):
        self.lag_alerts.append({"ts": datetime.utcnow().isoformat(), "lag_ms": lag_ms})
        logger.warning(f"LAG ALERT: {lag_ms}ms at {datetime.utcnow().isoformat()}")

    def _print_report(self, results: Dict):
        print("\n" + "=" * 55)
        print(f"  CDC Replication Health Report — {results['timestamp']}")
        print("=" * 55)

        lag = results.get("replication_lag") or {}
        lag_ms = lag.get("lag_ms", 0)
        lag_ok = "✓ OK" if lag_ms < THRESHOLDS["lag_ms"] else "✗ ALERT"
        print(f"  Replication Lag:   {lag_ms}ms".ljust(40) + lag_ok)

        for slot in results.get("replication_slots", []):
            slot_ok = "✓ OK" if slot["active"] and slot["retained_mb"] < THRESHOLDS["slot_retained_mb"] else "✗ ALERT"
            print(f"  Slot [{slot['slot_name']}]: {slot['retained_wal']} retained".ljust(40) + slot_ok)

        conns = results.get("active_connections", [])
        print(f"  Active Connections:{len(conns)}".ljust(40) + ("✓ OK" if conns else "⚠ NONE"))
        print("=" * 55 + "\n")


if __name__ == "__main__":
    monitor = LagMonitor()
    monitor.report()
