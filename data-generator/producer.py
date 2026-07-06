"""
Thin wrapper around confluent_kafka.Producer.

This is the actual ingestion point: the moment a StudentEvent crosses from
"something the generator decided happened" into "something durably sitting
in a Kafka topic that the rest of the system can see."
"""

from __future__ import annotations

import json
import logging

from confluent_kafka import Producer

from schemas import StudentEvent

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # Idempotent producer: avoids duplicate sends if a retry
                # happens after a broker ack was lost in transit.
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)

    def send(self, event: StudentEvent) -> None:
        """Publishes one event to its topic, keyed by student_id.

        Using student_id as the partition key guarantees every event for a
        given student lands in the same partition, keeping that student's
        history strictly ordered.
        """
        event.validate()
        self._producer.produce(
            topic=event.topic,
            key=event.student_id.encode("utf-8"),
            value=json.dumps(event.to_dict()).encode("utf-8"),
            callback=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        """Blocks until all outstanding sends are acknowledged."""
        self._producer.flush(timeout)
