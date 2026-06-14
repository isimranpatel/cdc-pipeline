# Runbook: Replication Slot Bloat

**Severity:** HIGH  
**Impact:** Disk exhaustion, PostgreSQL crash, data loss risk  
**Detection:** `diagnose_cdc.py` reports `retained_bytes > 512MB` or slot is inactive

---

## What Is Slot Bloat?

A PostgreSQL logical replication slot prevents WAL segments from being deleted until the consumer confirms it has read them. If the consumer (Debezium) goes down or falls behind, WAL accumulates indefinitely — filling the disk.

```sql
-- Check current slot status
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

---

## Resolution Steps

### Step 1 — Confirm the issue
```bash
python scripts/diagnose_cdc.py
```
Look for: `✗ SLOT BLOAT` or `✗ SLOT INACTIVE`

### Step 2 — Check if Debezium is running
```bash
curl http://localhost:8083/connectors/postgres-cdc-connector/status
```
If status is `FAILED`, restart it:
```bash
curl -X POST http://localhost:8083/connectors/postgres-cdc-connector/restart
```

### Step 3 — If Debezium cannot catch up (severe bloat)
Drop and recreate the slot. **Warning: events between drop and recreate will be lost — use snapshot mode.**
```sql
-- Drop the bloated slot
SELECT pg_drop_replication_slot('debezium_slot');

-- Recreate it
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
```

Then re-register Debezium with `snapshot.mode: always` to resync:
```bash
curl -X DELETE http://localhost:8083/connectors/postgres-cdc-connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json
```

### Step 4 — Prevent recurrence
Add a WAL retention limit to `postgresql.conf`:
```
max_slot_wal_keep_size = 2GB
```
This causes PostgreSQL to invalidate the slot rather than fill the disk.

---

## Post-Recovery Checklist
- [ ] Slot is active and consuming WAL
- [ ] `retained_wal` is decreasing
- [ ] Debezium connector shows `RUNNING`
- [ ] No gaps in consumer topic offsets
- [ ] Update this runbook with any new findings
