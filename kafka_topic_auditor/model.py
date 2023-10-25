"""Plain data structures describing topics and where their replicas live."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Partition:
    id: int
    leader: int | None  # None when the partition is offline (no leader)
    replicas: tuple[int, ...]  # assigned replicas, preferred leader first
    isr: tuple[int, ...]  # in-sync replicas


@dataclass
class Topic:
    name: str
    partitions: list[Partition] = field(default_factory=list)
    configs: dict[str, str] = field(default_factory=dict)

    @property
    def replication_factor(self) -> int:
        return max((len(p.replicas) for p in self.partitions), default=0)


@dataclass
class Cluster:
    topics: list[Topic] = field(default_factory=list)
    brokers: list[int] = field(default_factory=list)  # known live brokers; may be empty

    def broker_ids(self) -> list[int]:
        """Listed brokers plus every broker that hosts a replica."""
        ids = set(self.brokers)
        for topic in self.topics:
            for partition in topic.partitions:
                ids.update(partition.replicas)
        return sorted(ids)
