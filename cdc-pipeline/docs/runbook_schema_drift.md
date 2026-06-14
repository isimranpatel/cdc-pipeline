# Runbook: Schema Drift / Schema Migration Failures

**Severity:** MEDIUM–HIGH  
**Impact:** Consumer deserialization errors, data type mismatches, pipeline stall  
**Detection:** `event_processor.py` logs `SCHEMA DRIFT` warnings, consumer DLQ depth increases

---

## What Is Schema Drift?

Schema drift occurs when the source database schema changes (column added, renamed, type changed, dropped) without the CDC consumer being updated to handle the new structure. Debezium captures schema changes via the WAL but the consumer may reject events it can't parse.

---

## Detection

```bash
# Check DLQ for schema-related failures
python src/consumer.py --mode dlq-report

# Check event_processor logs for drift warnings
grep "SCHEMA DRIFT" logs/cdc.log | tail -20
```

---

## Resolution Steps

### Step 1 — Identify the schema change
```sql
-- Check current table schema
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'orders'
ORDER BY ordinal_position;
```

### Step 2 — Update EXPECTED_SCHEMAS in event_processor.py
```python
# src/event_processor.py
EXPECTED_SCHEMAS = {
    "orders": {
        "id", "customer_id", "product", "quantity",
        "amount", "status", "created_at", "updated_at",
        "new_column_name"   # ← add new columns here
    },
}
```

### Step 3 — Run migration on the sink database
```sql
-- Apply the same schema change to analytics store
ALTER TABLE orders ADD COLUMN new_column_name VARCHAR(100);
```

### Step 4 — Reprocess DLQ events
After fixing the schema, reprocess failed events from the dead letter queue:
```python
from src.dead_letter_queue import DeadLetterQueue

def fix_fn(event):
    # apply any needed transformation
    return event

dlq = DeadLetterQueue()
dlq.reprocess(fix_fn)
```

### Step 5 — Validate
```bash
python scripts/diagnose_cdc.py
# Expect: ✓ All checks passed
```

---

## Prevention
- Always apply schema changes to the sink **before** the source
- Use expand/contract pattern: add nullable column → backfill → add NOT NULL constraint
- Test schema changes in staging CDC environment first

---

## Post-Recovery Checklist
- [ ] `EXPECTED_SCHEMAS` updated in `event_processor.py`
- [ ] Sink schema migrated to match source
- [ ] DLQ events reprocessed successfully
- [ ] No new SCHEMA DRIFT warnings in logs
- [ ] Consumer lag back to zero
