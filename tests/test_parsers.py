import json
from pathlib import Path

import pytest

from kafka_topic_auditor.parsers import ParseError, cluster_to_dict, load_cluster, parse_describe_output

EXAMPLE = Path(__file__).resolve().parent.parent / "examples" / "topics-describe.txt"

# Real output of kafka-topics.sh --describe from Apache Kafka 4.4.0 (tab separated).
KAFKA_4 = (
    "Topic: orders\tTopicId: lsemsuPkSVWNroHNe0v7cA\tPartitionCount: 2\tReplicationFactor: 1\tConfigs: min.insync.replicas=1\n"
    "\tTopic: orders\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
    "\tTopic: orders\tPartition: 1\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: \tLastKnownElr: \n"
)


def test_parses_kafka_4_output_with_elr_columns():
    cluster = parse_describe_output(KAFKA_4)
    (topic,) = cluster.topics
    assert topic.name == "orders" and topic.replication_factor == 1
    assert topic.configs == {"min.insync.replicas": "1"}
    assert [(p.id, p.leader, p.replicas, p.isr) for p in topic.partitions] == [(0, 1, (1,), (1,)), (1, 1, (1,), (1,))]


def test_parses_older_output_with_offline_column_and_spaces_instead_of_tabs():
    text = """
Topic: payments  PartitionCount: 2  ReplicationFactor: 3  Configs: min.insync.replicas=2,cleanup.policy=compact,delete
    Topic: payments  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,1  Offline: 3
    Topic: payments  Partition: 1  Leader: none  Replicas: 3,1,2  Isr: 3  Offline: 1,2,3
"""
    (topic,) = parse_describe_output(text).topics
    assert topic.configs == {"min.insync.replicas": "2", "cleanup.policy": "compact,delete"}
    first, second = topic.partitions
    assert (first.leader, first.replicas, first.isr) == (2, (2, 3, 1), (2, 1))
    assert second.leader is None and second.isr == (3,)


def test_empty_isr_and_config_values_containing_colons():
    text = ("Topic: t\tPartitionCount: 1\tReplicationFactor: 2\tConfigs: follower.replication.throttled.replicas=0:1,0:2\n"
            "\tTopic: t\tPartition: 0\tLeader: none\tReplicas: 1,2\tIsr: \n")
    (topic,) = parse_describe_output(text).topics
    assert topic.configs == {"follower.replication.throttled.replicas": "0:1,0:2"}
    assert topic.partitions[0].isr == () and topic.partitions[0].leader is None


def test_example_file():
    cluster = load_cluster(EXAMPLE.read_text())
    assert [t.name for t in cluster.topics] == ["orders", "payments", "inventory", "clickstream", "audit-log",
                                                "user-profiles", "legacy-export"]
    assert cluster.broker_ids() == [1, 2, 3, 4]
    assert sum(len(t.partitions) for t in cluster.topics) == 28


def test_json_snapshot_round_trip():
    cluster = load_cluster(EXAMPLE.read_text())
    cluster.brokers = [1, 2, 3, 4, 5]
    again = load_cluster(json.dumps(cluster_to_dict(cluster)))
    assert again == cluster


def test_bad_input():
    with pytest.raises(ParseError):
        load_cluster("this is not kafka output\n")
    with pytest.raises(ParseError):
        load_cluster('{"topics": [{"partitions": []}]}')
    with pytest.raises(ParseError):
        parse_describe_output("\tTopic: t\tPartition: zero\tLeader: 1\tReplicas: 1\tIsr: 1\n")
