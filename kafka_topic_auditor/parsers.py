"""Read a cluster snapshot from JSON or from `kafka-topics --describe` text output."""

from __future__ import annotations

import json
import re

from .model import Cluster, Partition, Topic


class ParseError(ValueError):
    """Raised when the input cannot be read."""


_KEYS = ("TopicId", "Topic", "PartitionCount", "ReplicationFactor", "Configs", "Partition", "Leader",
         "Replicas", "Isr", "Elr", "LastKnownElr", "Offline", "Adding Replicas", "Removing Replicas")
_KEY_RE = re.compile(r"(?:^|\s)(" + "|".join(_KEYS) + r"):(?=\s|$)")


def _fields(line: str) -> dict[str, str]:
    """Split a describe line into its `Key: value` fields.

    Kafka separates fields with tabs; copies pasted from a terminal often turn those
    into spaces, so fall back to locating the known keys.
    """
    if "\t" in line.strip():
        pairs = (part.split(":", 1) for part in line.strip().split("\t") if ":" in part)
        return {key.strip(): value.strip() for key, value in pairs}
    pieces = _KEY_RE.split(line)
    return {pieces[i]: pieces[i + 1].strip() for i in range(1, len(pieces) - 1, 2)}


def _id_list(value: str) -> tuple[int, ...]:
    return tuple(int(v) for v in value.replace(" ", "").split(",") if v)


def _configs(value: str) -> dict[str, str]:
    configs: dict[str, str] = {}
    last = None
    for item in value.split(","):
        if "=" in item:
            last, _, val = item.partition("=")
            last = last.strip()
            configs[last] = val.strip()
        elif last is not None and item:  # a list value that itself contains commas
            configs[last] += "," + item.strip()
    return configs


def parse_describe_output(text: str) -> Cluster:
    """Parse `kafka-topics --describe` output for any number of topics."""
    topics: dict[str, Topic] = {}
    for number, raw in enumerate(text.splitlines(), 1):
        if "Topic:" not in raw:
            continue
        fields = _fields(raw)
        name = fields.get("Topic")
        if not name:
            continue
        topic = topics.setdefault(name, Topic(name))
        try:
            if "Partition" in fields:
                leader = fields.get("Leader", "").lower()
                topic.partitions.append(Partition(
                    id=int(fields["Partition"]),
                    leader=None if leader in ("", "none", "-1") else int(leader),
                    replicas=_id_list(fields.get("Replicas", "")),
                    isr=_id_list(fields.get("Isr", "")),
                ))
            elif "Configs" in fields:
                topic.configs.update(_configs(fields["Configs"]))
        except ValueError as exc:
            raise ParseError(f"line {number}: cannot parse {raw.strip()!r}") from exc
    if not topics:
        raise ParseError("no topics found (expected JSON or kafka-topics --describe output)")
    return Cluster(topics=list(topics.values()))


def cluster_from_dict(data: dict) -> Cluster:
    """Build a cluster from the JSON snapshot structure documented in the README."""
    try:
        topics = []
        for t in data["topics"]:
            partitions = [
                Partition(
                    id=int(p["partition"]),
                    leader=None if p.get("leader") in (None, -1, "none") else int(p["leader"]),
                    replicas=tuple(int(r) for r in p["replicas"]),
                    isr=tuple(int(r) for r in p.get("isr", [])),
                )
                for p in t.get("partitions", [])
            ]
            configs = {str(k): str(v) for k, v in (t.get("configs") or {}).items() if v is not None}
            topics.append(Topic(str(t["name"]), partitions, configs))
        return Cluster(topics=topics, brokers=[int(b) for b in data.get("brokers", [])])
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid snapshot JSON: {exc!r}") from exc


def cluster_to_dict(cluster: Cluster) -> dict:
    return {
        "brokers": cluster.brokers,
        "topics": [
            {
                "name": t.name,
                "configs": t.configs,
                "partitions": [
                    {"partition": p.id, "leader": p.leader, "replicas": list(p.replicas), "isr": list(p.isr)}
                    for p in t.partitions
                ],
            }
            for t in cluster.topics
        ],
    }


def load_cluster(text: str) -> Cluster:
    """Detect the format (JSON snapshot or kafka-topics text) and parse it."""
    if text.lstrip().startswith("{"):
        try:
            return cluster_from_dict(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ParseError(f"invalid JSON: {exc}") from exc
    return parse_describe_output(text)
