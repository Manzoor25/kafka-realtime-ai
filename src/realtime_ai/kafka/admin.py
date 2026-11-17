"""Kafka cluster topic bootstrapper and Karapace Schema Registry initializer."""

from __future__ import annotations

import logging
import time

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

from realtime_ai.config import Settings
from realtime_ai.kafka.topics import ALL_TOPICS
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


def wait_for_kafka(bootstrap_servers: str, timeout_seconds: float = 90.0) -> AdminClient:
    deadline = time.monotonic() + timeout_seconds
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            meta = admin.list_topics(timeout=5.0)
            if len(meta.brokers) >= 1:
                logger.info(
                    "Connected to Kafka cluster (%d broker(s) online)", len(meta.brokers)
                )
                return admin
        except Exception as exc:
            last_err = exc
        time.sleep(1.5)
    raise TimeoutError(f"Kafka cluster unreachable at {bootstrap_servers}: {last_err}")


def ensure_topics_and_schemas(settings: Settings, codec: SchemaRegistryCodec) -> dict[str, int]:
    """Ensures all topics exist with desired configs and registers all JSON Schemas."""
    if settings.kafka_bootstrap_servers.startswith("memory://"):
        return codec.register_all()

    admin = wait_for_kafka(settings.kafka_bootstrap_servers)
    metadata = admin.list_topics(timeout=10.0)
    broker_count = max(1, len(metadata.brokers))
    replication_factor = min(3, broker_count)
    min_isr = "2" if replication_factor >= 3 else "1"

    new_topics: list[NewTopic] = []
    for spec in ALL_TOPICS:
        if spec.name in metadata.topics and metadata.topics[spec.name].error is None:
            continue
        cfg = {
            "cleanup.policy": spec.cleanup_policy,
            "min.insync.replicas": min_isr,
        }
        if spec.retention_ms != -1:
            cfg["retention.ms"] = str(spec.retention_ms)
        cfg.update(spec.extra_config)
        new_topics.append(
            NewTopic(
                topic=spec.name,
                num_partitions=spec.partitions,
                replication_factor=replication_factor,
                config=cfg,
            )
        )

    if new_topics:
        futures = admin.create_topics(new_topics, request_timeout=20.0)
        for topic_name, fut in futures.items():
            try:
                fut.result()
                logger.info("Created Kafka topic %s (RF=%d)", topic_name, replication_factor)
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    # Ensure dynamic group config for KIP-932 share group so pre-produced records are read
    try:
        group_res = ConfigResource(
            ResourceType.GROUP,
            settings.share_group_triage,
            set_config={
                "share.auto.offset.reset": "earliest",
                "share.record.lock.duration.ms": "20000",
                "share.delivery.count.limit": "10",
            },
        )
        for _, fut in admin.alter_configs([group_res]).items():
            fut.result(timeout=10.0)
        logger.info("Configured KIP-932 share group '%s'", settings.share_group_triage)
    except Exception as exc:
        logger.warning("Share group dynamic config alter skipped/deferred: %s", exc)

    # Wait briefly for Karapace Schema Registry and register all JSON Schemas
    deadline = time.monotonic() + 60.0
    while True:
        try:
            registered = codec.register_all()
            logger.info("Registered %d JSON Schemas in Schema Registry", len(registered))
            return registered
        except Exception as exc:
            if time.monotonic() >= deadline:
                raise
            logger.info("Waiting for Schema Registry (%s)...", exc)
            time.sleep(1.5)
