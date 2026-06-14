# Runbook: Missed Transactions / LSN Gap

**Severity:** HIGH  
**Impact:** Data inconsistency between source and sink  
**Detection:** LSN gap detected in `diagnose_cdc.py --check-lsn-gaps`, or consumer reports sequence errors

---

## What Are Missed Transactions?

A missed transaction occurs when there is a gap in the Log Sequence Number (LSN) sequence consumed by the CDC pipeline. This can result in the analytics store being out of sync with the source database.

**Common causes:**
- Debezium connector crashed mid-stream
- Kafka topic offset manually reset
- Replication slot dropped and recreated without full resnapshot
- WAL corruption (rare)

---

## Detection

```bash
# Check LSN gap
python scripts/diagnose_cdc.py --check-lsn-gaps

# Manual SQL check
psql -U postgres -d sourcedb -c "
SELECT
    slot_name,
    restart_lsn,
    confirmed_flush_lsn,
    pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn) AS gap_bytes
FROM pg_replication_slots;"
```

---

## Resolution Steps

### Step 1 — Identify the gap window
```sql
-- Find the time window of the LSN gap
SELECT pg_walfile_name(restart_lsn), pg_walfile_name(confirmed_flush_lsn)
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
```

### Step 2 — Check if data is recoverable from WAL
```bash
# Decode WAL in the gap range (requires pg_waldump)
pg_waldump -p /var/lib/postgresql/data/pg_wal \
  --start=<restart_lsn> --end=<confirmed_flush_lsn>
```

### Step 3 — If WAL is available, replay manually
Extract the affected rows from WAL output and re-apply to the sink:
```python
# Use event_processor.py to manually replay
from src.event_processor import EventProcessor
proc = EventProcessor()
proc.handle_insert("orders", recovered_row)
```

### Step 4 — If WAL is not available, resnapshot
Trigger a full resnapshot of affected tables:
```bash
# Update connector to snapshot mode
curl -X PUT http://localhost:8083/connectors/postgres-cdc-connector/config \
  -H "Content-Type: application/json" \
  -d '{"snapshot.mode": "always", ...}'
```

### Step 5 — Validate data consistency
```sql
-- Row count comparison between source and sink
SELECT 'source' AS db, COUNT(*) FROM sourcedb.public.orders
UNION ALL
SELECT 'sink'   AS db, COUNT(*) FROM analyticsdb.public.orders;

-- Checksum comparison on key columns
SELECT SUM(amount) FROM sourcedb.public.orders WHERE status = 'completed';
SELECT SUM(amount) FROM analyticsdb.public.orders WHERE status = 'completed';
```

---

## Post-Recovery Checklist
- [ ] LSN gap is closed (confirmed_flush_lsn = current WAL LSN)
- [ ] Row counts match between source and sink
- [ ] Checksum validation passed
- [ ] Consumer lag is zero
- [ ] Downstream alerts cleared
