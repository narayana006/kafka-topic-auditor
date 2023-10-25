"""Optional adapter: read topics, partition state and topic configs from a live cluster.

Uses confluent-kafka and only read-only calls: Metadata and DescribeConfigs.
"""

from __future__ import annotations

from .model import Cluster, Partition, Topic


def fetch_cluster(bootstrap: str, client_config: dict | None = None, timeout: float = 15.0) -> Cluster:
    try:
        from confluent_kafka.admin import AdminClient, ConfigResource
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(
            "reading from a live cluster needs the confluent-kafka package: pip install 'kafka-topic-auditor[live]'"
        ) from exc

    admin = AdminClient({"bootstrap.servers": bootstrap, **(client_config or {})})
    metadata = admin.list_topics(timeout=timeout)

    topics: dict[str, Topic] = {}
    for name, meta in sorted(metadata.topics.items()):
        partitions = [
            Partition(id=pid, leader=None if p.leader < 0 else p.leader, replicas=tuple(p.replicas), isr=tuple(p.isrs))
            for pid, p in sorted(meta.partitions.items())
        ]
        topics[name] = Topic(name, partitions)

    if topics:
        resources = [ConfigResource(ConfigResource.Type.TOPIC, name) for name in topics]
        for resource, future in admin.describe_configs(resources, request_timeout=timeout).items():
            entries = future.result()
            # Effective values, including those inherited from the broker configuration.
            topics[resource.name].configs = {
                key: entry.value for key, entry in entries.items() if entry.value is not None
            }
    return Cluster(topics=list(topics.values()), brokers=sorted(metadata.brokers))
