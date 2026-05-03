# ADR-001: Kafka in KRaft Mode (No Zookeeper)

## Status
Accepted

## Context
Kafka traditionally requires Zookeeper for cluster metadata management. Since Kafka 3.3, KRaft mode allows Kafka to manage its own metadata without Zookeeper, reducing operational complexity.

## Decision
Use Kafka in KRaft mode (confluentinc/cp-kafka:7.6.0) without a separate Zookeeper container.

## Consequences
**Positive:**
- One fewer container to manage (saves ~500MB RAM)
- Simpler docker-compose configuration
- Faster startup (no Zookeeper coordination)
- Aligns with Kafka's future direction (Zookeeper is deprecated)

**Negative:**
- KRaft is newer — some edge cases may be less battle-tested
- Some older Kafka tooling assumes Zookeeper exists

**Trade-off accepted:** For a local dev environment with a single broker, KRaft is simpler and sufficient. Production Kafka clusters are also migrating to KRaft.
