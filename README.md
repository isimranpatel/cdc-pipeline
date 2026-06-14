# PostgreSQL CDC Replication Pipeline

A production-style **Change Data Capture (CDC)** pipeline using PostgreSQL logical replication (WAL), Debezium, and Apache Kafka to stream real-time database changes to a downstream analytics store — with automated failure detection, lag monitoring, and runbook-documented recovery procedures.

---

## Architecture

```
PostgreSQL (WAL / pgoutput)
        │
        ▼
  Debezium Connector
  (Kafka Connect)
        │
        ▼
  Apache Kafka Topic
  (cdc.public.orders)
        │
        ▼
  Python Consumer
  (CDC Event Processor)
        │
   ┌────┴────┐
   ▼         ▼
Analytics   Dead Letter
  Store      Queue
(PostgreSQL) (failed events)
        │
        ▼
  CloudWatch-style
    Monitoring
  (Prometheus + logs)
```

---

## Features

- **PostgreSQL WAL-based CDC** — captures every INSERT, UPDATE, DELETE in real time using logical replication slots
- **Debezium integration** — battle-tested CDC connector handling schema changes, LOB columns, and replication slot management
- **Kafka streaming** — decoupled, fault-tolerant event streaming with configurable retention
- **Python CDC consumer** — processes change events with schema validation, deduplication, and dead-letter queue
- **Lag monitoring** — tracks replication lag, slot bloat, and consumer lag with alerting thresholds
- **Failure diagnosis scripts** — automated detection of common CDC failure modes (slot overflow, missed transactions, schema drift)
- **Runbooks** — documented recovery procedures for every known failure mode

---

## Tech Stack

| Component | Technology |
|---|---|
| Source Database | PostgreSQL 15 |
| CDC Engine | Debezium 2.4 |
| Message Broker | Apache Kafka 3.5 |
| Stream Processing | Python 3.11, kafka-python |
| Sink / Analytics Store | PostgreSQL (replica) |
| Monitoring | Prometheus + custom log parser |
| Infrastructure | Docker Compose |
| Language | Python, SQL, Bash |

---

## Project Structure

```
cdc-pipeline/
├── config/
│   ├── debezium-connector.json       # Debezium connector configuration
│   ├── kafka-connect.properties      # Kafka Connect worker config
│   └── prometheus.yml                # Monitoring config
├── src/
│   ├── consumer.py                   # Kafka CDC event consumer
│   ├── event_processor.py            # Schema validation & transformation
│   ├── lag_monitor.py                # Replication lag & slot monitoring
│   └── dead_letter_queue.py          # Failed event handler
├── scripts/
│   ├── setup_postgres.sql            # Enable WAL, create replication slot
│   ├── diagnose_cdc.py               # CDC failure diagnostic tool
│   ├── check_slot_bloat.sql          # Detect replication slot bloat
│   └── simulate_load.py              # Generate test CDC events
├── tests/
│   ├── test_consumer.py              # Unit tests for consumer
│   ├── test_event_processor.py       # Unit tests for processor
│   └── test_lag_monitor.py           # Unit tests for monitor
├── docs/
│   ├── runbook_slot_bloat.md         # Recovery: replication slot overflow
│   ├── runbook_missed_transactions.md# Recovery: missed transactions
│   ├── runbook_schema_drift.md       # Recovery: schema migration failures
│   └── architecture.md              # Detailed architecture notes
├── docker-compose.yml                # Full stack: PG + Kafka + Debezium
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### 1. Start the full stack
```bash
git clone https://github.com/isimranpatel/cdc-pipeline.git
cd cdc-pipeline
docker-compose up -d
```

### 2. Set up PostgreSQL WAL replication
```bash
docker exec -it postgres psql -U postgres -f /scripts/setup_postgres.sql
```

### 3. Register the Debezium connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json
```

### 4. Start the CDC consumer
```bash
pip install -r requirements.txt
python src/consumer.py
```

### 5. Simulate CDC events
```bash
python scripts/simulate_load.py --events 1000 --interval 0.1
```

### 6. Monitor replication lag
```bash
python src/lag_monitor.py --threshold-ms 500
```

---

## CDC Failure Modes & Diagnostics

### Automatic Detection
```bash
python scripts/diagnose_cdc.py
```

Detects and reports:
- Replication slot bloat (unconsumed WAL accumulation)
- Consumer lag exceeding threshold
- Missed transactions (gap in LSN sequence)
- Schema drift between source and sink
- Dead letter queue buildup

### Common Failure Modes

| Failure | Symptom | Runbook |
|---|---|---|
| Slot bloat | Disk filling up, WAL retained indefinitely | [docs/runbook_slot_bloat.md](docs/runbook_slot_bloat.md) |
| Missed transactions | LSN gap in consumer offset | [docs/runbook_missed_transactions.md](docs/runbook_missed_transactions.md) |
| Schema drift | Consumer deserialization errors | [docs/runbook_schema_drift.md](docs/runbook_schema_drift.md) |
| Consumer lag | Events backed up in Kafka topic | Restart consumer, check `lag_monitor.py` |

---

## Monitoring

The lag monitor tracks:
- **Replication lag** (ms) — time between WAL write and consumer processing
- **Slot retained bytes** — WAL accumulation in the replication slot
- **Consumer offset lag** — messages behind in Kafka topic
- **Dead letter queue depth** — failed events awaiting reprocessing

```bash
# View current replication status
python src/lag_monitor.py --report

# Output example:
# Replication Lag:     42ms      ✓ OK
# Slot Retained:       128MB     ✓ OK
# Consumer Lag:        0 msgs    ✓ OK
# DLQ Depth:           0 events  ✓ OK
```

---

## Key SQL Queries

```sql
-- Check replication slot status
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

-- Check replication lag
SELECT now() - pg_last_xact_replay_timestamp() AS replication_lag;

-- Active replication connections
SELECT pid, usename, application_name, state, sent_lsn, write_lsn, flush_lsn
FROM pg_stat_replication;
```

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Interview Talking Points

This project demonstrates:
1. **WAL internals** — how PostgreSQL logical replication works at the storage layer
2. **CDC failure diagnosis** — slot bloat, LSN gaps, schema drift — and how to resolve each
3. **Production patterns** — dead letter queues, retry logic, lag alerting
4. **Runbook culture** — every failure mode documented with step-by-step recovery

---

## License
MIT
