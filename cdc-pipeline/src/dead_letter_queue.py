"""
dead_letter_queue.py
--------------------
Routes failed CDC events to a dead letter Kafka topic.
Supports reprocessing failed events after fixing the root cause.
"""

import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("cdc.dlq")

DLQ_TOPIC = "cdc.dead.letter"
KAFKA_BOOTSTRAP = "localhost:9092"


class DeadLetterQueue:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        self.dlq_count = 0

    def send(self, original_message, error: str):
        """Send a failed message to the dead letter queue with error context."""
        dlq_event = {
            "original_topic": original_message.topic,
            "original_partition": original_message.partition,
            "original_offset": original_message.offset,
            "original_value": original_message.value,
            "error": error,
            "failed_at": datetime.utcnow().isoformat(),
            "retry_count": 0,
        }
        try:
            self.producer.send(DLQ_TOPIC, value=dlq_event)
            self.producer.flush()
            self.dlq_count += 1
            logger.warning(
                f"Event sent to DLQ — topic={original_message.topic} "
                f"offset={original_message.offset} error={error}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send event to DLQ: {e}")

    def reprocess(self, fix_fn):
        """
        Replay events from DLQ after applying a fix function.
        fix_fn: callable that takes a raw event dict and returns corrected event.
        """
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            DLQ_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="cdc-dlq-reprocessor",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )
        reprocessed = 0
        failed_again = 0
        for message in consumer:
            try:
                fixed_event = fix_fn(message.value)
                logger.info(f"Reprocessed DLQ event: offset={message.offset}")
                reprocessed += 1
            except Exception as e:
                logger.error(f"Reprocessing failed again: {e}")
                failed_again += 1
        consumer.close()
        logger.info(f"DLQ reprocess complete — reprocessed: {reprocessed}, still failing: {failed_again}")
        return {"reprocessed": reprocessed, "failed": failed_again}

    @property
    def depth(self) -> int:
        return self.dlq_count
