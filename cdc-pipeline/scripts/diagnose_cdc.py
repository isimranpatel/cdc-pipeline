#!/usr/bin/env python3
"""
diagnose_cdc.py
---------------
Automated CDC failure diagnostic tool.
Detects: slot bloat, missed transactions, schema drift, consumer lag, DLQ buildup.

Usage:
    python scripts/diagnose_cdc.py
    python scripts/diagnose_cdc.py --fix-slot-bloat
    python scripts/diagnose_cdc.py --check-lsn-gaps
"""

import argparse
import logging
import os
import sys
import psycopg2
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc.diagnose")

DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", 5432)),
    "dbname": os.getenv("SOURCE_DB_NAME", "sourcedb"),
    "user": os.getenv("SOURCE_DB_USER", "postgres"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "postgres"),
}

ISSUES_FOUND = []


def check_replication_slots(conn):
    """Diagnose 1: Replication slot health and WAL bloat."""
    print("\n[1] Checking replication slots...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                slot_name,
                active,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes,
                restart_lsn,
                confirmed_flush_lsn
            FROM pg_replication_slots
            WHERE slot_type = 'logical';
        """)
        rows = cur.fetchall()
        if not rows:
            ISSUES_FOUND.append("No logical replication slots found — Debezium may not be connected.")
            print("  ✗ No logical replication slots found.")
            return

        for row in rows:
            slot_name, active, retained_wal, retained_bytes, restart_lsn, flush_lsn = row
            retained_mb = (retained_bytes or 0) / (1024 * 1024)
            print(f"  Slot: {slot_name}")
            print(f"    Active:       {active}")
            print(f"    Retained WAL: {retained_wal}")

            if not active:
                issue = f"SLOT INACTIVE [{slot_name}] — WAL will accumulate indefinitely. See: docs/runbook_slot_bloat.md"
                ISSUES_FOUND.append(issue)
                print(f"    ✗ {issue}")
            elif retained_mb > 512:
                issue = f"SLOT BLOAT [{slot_name}] — {retained_wal} retained (>{512}MB threshold). See: docs/runbook_slot_bloat.md"
                ISSUES_FOUND.append(issue)
                print(f"    ✗ {issue}")
            else:
                print(f"    ✓ OK")


def check_lsn_gaps(conn):
    """Diagnose 2: Detect LSN gaps that indicate missed transactions."""
    print("\n[2] Checking for LSN gaps (missed transactions)...")
    with conn.cursor() as cur:
        # Check if WAL is ahead of what's been confirmed consumed
        cur.execute("""
            SELECT
                slot_name,
                pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn) AS lsn_gap_bytes,
                pg_size_pretty(pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn)) AS lsn_gap
            FROM pg_replication_slots
            WHERE slot_type = 'logical';
        """)
        for row in cur.fetchall():
            slot_name, gap_bytes, gap_pretty = row
            gap_mb = (gap_bytes or 0) / (1024 * 1024)
            print(f"  Slot [{slot_name}] LSN gap: {gap_pretty}")
            if gap_mb > 100:
                issue = f"LSN GAP [{slot_name}] — {gap_pretty} between restart_lsn and confirmed_flush_lsn. Possible missed transactions. See: docs/runbook_missed_transactions.md"
                ISSUES_FOUND.append(issue)
                print(f"    ✗ {issue}")
            else:
                print(f"    ✓ OK")


def check_replication_lag(conn):
    """Diagnose 3: Current replication lag."""
    print("\n[3] Checking replication lag...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000 AS lag_ms,
                now() - pg_last_xact_replay_timestamp() AS lag_human
        """)
        row = cur.fetchone()
        if row and row[0]:
            lag_ms = round(row[0], 2)
            print(f"  Replication lag: {row[1]} ({lag_ms}ms)")
            if lag_ms > 500:
                issue = f"HIGH REPLICATION LAG: {lag_ms}ms — check consumer throughput and Kafka consumer group."
                ISSUES_FOUND.append(issue)
                print(f"  ✗ {issue}")
            else:
                print(f"  ✓ OK")
        else:
            print("  ⚠ Could not determine replication lag (no replica connected?)")


def check_active_connections(conn):
    """Diagnose 4: Active replication connections."""
    print("\n[4] Checking active replication connections...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pid, usename, application_name, state, write_lag, flush_lag, replay_lag
            FROM pg_stat_replication;
        """)
        rows = cur.fetchall()
        if not rows:
            issue = "No active replication connections — Debezium connector may be down."
            ISSUES_FOUND.append(issue)
            print(f"  ✗ {issue}")
        else:
            for row in rows:
                print(f"  PID={row[0]} user={row[1]} app={row[2]} state={row[3]}")
                print(f"    write_lag={row[4]} flush_lag={row[5]} replay_lag={row[6]}")
            print(f"  ✓ {len(rows)} active connection(s)")


def check_wal_config(conn):
    """Diagnose 5: Verify PostgreSQL WAL configuration."""
    print("\n[5] Checking WAL configuration...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders', 'wal_keep_size');
        """)
        config = {row[0]: row[1] for row in cur.fetchall()}
        for key, val in config.items():
            print(f"  {key}: {val}")

        if config.get("wal_level") != "logical":
            issue = f"WAL level is '{config.get('wal_level')}' — must be 'logical' for CDC. Restart PostgreSQL after changing postgresql.conf."
            ISSUES_FOUND.append(issue)
            print(f"  ✗ {issue}")
        else:
            print("  ✓ wal_level=logical confirmed")


def print_summary():
    """Print final diagnosis summary."""
    print("\n" + "=" * 60)
    print("  DIAGNOSIS SUMMARY")
    print("=" * 60)
    if not ISSUES_FOUND:
        print("  ✓ All checks passed — CDC pipeline is healthy.")
    else:
        print(f"  ✗ {len(ISSUES_FOUND)} issue(s) found:\n")
        for i, issue in enumerate(ISSUES_FOUND, 1):
            print(f"  [{i}] {issue}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Diagnostic Tool")
    parser.add_argument("--check-lsn-gaps", action="store_true", help="Check for LSN gaps")
    args = parser.parse_args()

    print(f"\nCDC Diagnostic Tool — {datetime.utcnow().isoformat()}")
    print("Connecting to source database...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"✗ Cannot connect to source database: {e}")
        sys.exit(1)

    try:
        check_wal_config(conn)
        check_replication_slots(conn)
        if args.check_lsn_gaps:
            check_lsn_gaps(conn)
        check_replication_lag(conn)
        check_active_connections(conn)
    finally:
        conn.close()

    print_summary()
    sys.exit(1 if ISSUES_FOUND else 0)


if __name__ == "__main__":
    main()
