"""
consumer.py
-----------
Kafka CDC event consumer. Reads change events from Debezium topics,
validates schema, processes events, and writes to analytics store.
Failed events are routed to the dead letter queue.
"""

import json
import logging
import signal
import sys
from datetime import datetime
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from event_processor import EventProcessor
from dead_letter_queue import DeadLetterQueue
from lag_monitor import LagMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc.consumer")

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["cdc.public.orders", "cdc.public.customers"]
GROUP_ID = "cdc-analytics-consumer"
LAG_ALERT_THRESHOLD_MS = 500  # alert if replication lag > 500ms


class CDCConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            max_poll_records=500,
            session_timeout_ms=30000,
        )
        self.processor = EventProcessor()
        self.dlq = DeadLetterQueue()
        self.monitor = LagMonitor()
        self.running = True
        self.stats = {"processed": 0, "failed": 0, "skipped": 0}

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def run(self):
        logger.info(f"Starting CDC consumer on topics: {TOPICS}")
        logger.info(f"Group ID: {GROUP_ID}")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                self._process_message(message)

                # Commit offset only after successful processing
                self.consumer.commit()

                # Log stats every 100 events
                if self.stats["processed"] % 100 == 0 and self.stats["processed"] > 0:
                    self._log_stats()

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            raise
        finally:
            self._cleanup()

    def _process_message(self, message):
        try:
            payload = message.value

            # Skip tombstone events (null payload = delete marker)
            if payload is None:
                self.stats["skipped"] += 1
                return

            event = payload.get("payload", {})
            op = event.get("op")  # c=create, u=update, d=delete, r=read(snapshot)

            if not op:
                self.stats["skipped"] += 1
                return

            # Check replication lag
            source_ts = event.get("source", {}).get("ts_ms", 0)
            lag_ms = self._calculate_lag(source_ts)
            if lag_ms > LAG_ALERT_THRESHOLD_MS:
                logger.warning(f"High replication lag detected: {lag_ms}ms")
                self.monitor.record_lag_alert(lag_ms)

            # Process by operation type
            table = message.topic.split(".")[-1]

            if op == "c":
                self.processor.handle_insert(table, event.get("after", {}))
            elif op == "u":
                self.processor.handle_update(table, event.get("before", {}), event.get("after", {}))
            elif op == "d":
                self.processor.handle_delete(table, event.get("before", {}))
            elif op == "r":
                self.processor.handle_snapshot(table, event.get("after", {}))

            self.stats["processed"] += 1

        except Exception as e:
            logger.error(f"Failed to process message offset={message.offset}: {e}")
            self.dlq.send(message, error=str(e))
            self.stats["failed"] += 1

    def _calculate_lag(self, source_ts_ms: int) -> int:
        """Calculate replication lag in milliseconds."""
        if not source_ts_ms:
            return 0
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        return max(0, now_ms - source_ts_ms)

    def _log_stats(self):
        logger.info(
            f"Stats — processed: {self.stats['processed']} | "
            f"failed: {self.stats['failed']} | "
            f"skipped: {self.stats['skipped']}"
        )

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Stopping consumer...")
        self.running = False

    def _cleanup(self):
        logger.info("Closing consumer...")
        self.consumer.close()
        self._log_stats()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    consumer = CDCConsumer()
    consumer.run()
