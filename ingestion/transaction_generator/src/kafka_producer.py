"""
Kafka producer for transaction events.

Serializes TransactionEvent instances to JSON and publishes them to a
Kafka topic. Handles connection management, serialization, and basic
error handling.

Usage:
    producer = TransactionKafkaProducer(bootstrap_servers="localhost:9092")
    producer.send(event)
    producer.flush()
    producer.close()
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.transaction_generator.src.schemas import TransactionEvent


# ---------------------------------------------------------------------------
# Custom JSON encoder for types that json.dumps cannot handle natively
# ---------------------------------------------------------------------------

class _EventEncoder(json.JSONEncoder):
    """Handle Decimal, datetime, and UUID serialization."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class TransactionKafkaProducer:
    """
    Thin wrapper around kafka-python's KafkaProducer.

    Why a wrapper instead of using KafkaProducer directly?
    1. Encapsulates serialization logic (Decimal, datetime handling)
    2. Provides a clean interface for the generator to call
    3. Adds delivery callbacks for observability
    4. Makes it easy to swap Kafka for another broker later (single change point)
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions.raw",
    ) -> None:
        self._topic = topic
        self._sent_count = 0
        self._error_count = 0

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, cls=_EventEncoder).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",               # Wait for all replicas (strongest durability)
            retries=3,                # Retry on transient failures
            max_in_flight_requests_per_connection=1,  # Preserve ordering
            linger_ms=10,             # Batch for 10ms before sending (throughput vs latency)
            batch_size=32768,         # 32KB batch size
        )

    # ----- public API -----

    def send(self, event: TransactionEvent) -> None:
        """
        Send a single transaction event to Kafka.

        Uses user_id as the partition key so all events for the same user
        land in the same partition. This guarantees per-user ordering,
        which is critical for downstream fraud feature computation
        (e.g., rolling windows per user).
        """
        try:
            self._producer.send(
                self._topic,
                key=event.user_id,
                value=event.model_dump(mode="python"),
                timestamp_ms=int(event.event_timestamp.timestamp() * 1000),
            ).add_callback(self._on_success).add_errback(self._on_error)
            self._sent_count += 1
        except KafkaError as e:
            self._error_count += 1
            print(f"[ERROR] Failed to send event {event.transaction_id}: {e}", file=sys.stderr)

    def flush(self) -> None:
        """Block until all buffered messages are sent."""
        self._producer.flush()

    def close(self) -> None:
        """Flush and close the producer."""
        self._producer.flush()
        self._producer.close()

    @property
    def stats(self) -> dict:
        """Return send/error counts for monitoring."""
        return {
            "sent": self._sent_count,
            "errors": self._error_count,
            "topic": self._topic,
        }

    # ----- callbacks -----

    def _on_success(self, metadata) -> None:
        """Called when a message is successfully delivered."""
        pass  # Silent success; we track via self._sent_count

    def _on_error(self, exc) -> None:
        """Called when a message delivery fails."""
        self._error_count += 1
        print(f"[ERROR] Delivery failed: {exc}", file=sys.stderr)
