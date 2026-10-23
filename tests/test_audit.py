import pytest

from kafka_topic_auditor import Cluster, Partition, Rules, Topic, audit
from kafka_topic_auditor.rules import RulesError, load_rules


def topic(name="t", rf=3, parts=3, min_isr=None, configs=None, isr=None, leader="first"):
    """A healthy topic on brokers 1..rf with leaders rotated, unless overridden."""
    brokers = list(range(1, rf + 1))
    partitions = []
    for i in range(parts):
        replicas = tuple(brokers[i % rf:] + brokers[:i % rf])
        partitions.append(Partition(i, replicas[0] if leader == "first" else leader, replicas,
                                    tuple(isr) if isr is not None else replicas))
    cfg = dict(configs or {})
    if min_isr is not None:
        cfg["min.insync.replicas"] = str(min_isr)
    return Topic(name, partitions, cfg)


def rules_of(findings):
    return [f.rule for f in findings]


def test_healthy_cluster_has_no_findings():
    assert audit(Cluster([topic("a", min_isr=2), topic("b", min_isr=2, parts=6)])) == []


def test_replication_factor_below_minimum():
    findings = audit(Cluster([topic(rf=2, parts=2, min_isr=1)]))
    assert rules_of(findings) == ["rf-below-minimum"]
    assert "Replication factor is 2" in findings[0].message
    assert audit(Cluster([topic(rf=2, parts=2, min_isr=1)]), Rules(min_rf=2, min_isr=1)) == []


def test_single_replica_topic_reports_only_the_replication_factor():
    findings = audit(Cluster([topic(rf=1, parts=1, min_isr=1)]))
    assert rules_of(findings) == ["rf-below-minimum"]
    assert "single broker failure" in findings[0].message


def test_min_isr_too_low_uses_default_when_not_configured():
    findings = audit(Cluster([topic()]))  # no min.insync.replicas listed -> Kafka default of 1
    assert rules_of(findings) == ["min-isr-too-low"]
    assert "min.insync.replicas=1" in findings[0].message
    assert "--add-config min.insync.replicas=2" in findings[0].fix
    assert audit(Cluster([topic()]), Rules(default_min_isr=2)) == []


def test_min_isr_equal_to_rf_leaves_no_headroom():
    assert rules_of(audit(Cluster([topic(min_isr=3)]))) == ["min-isr-no-headroom"]


def test_min_isr_above_rf_is_critical_and_not_reported_as_under_min_isr():
    findings = audit(Cluster([topic(min_isr=4)]))
    assert rules_of(findings) == ["min-isr-above-rf"]
    assert findings[0].severity == "critical"
    assert rules_of(audit(Cluster([topic(rf=1, parts=1, min_isr=2)]), Rules(min_rf=1))) == ["min-isr-above-rf"]


def test_two_replicas_with_min_isr_one_is_not_flagged_as_too_low():
    # min.insync.replicas=2 would leave no headroom on RF 2, so only the RF is the problem.
    assert rules_of(audit(Cluster([topic(rf=2, min_isr=1)]))) == ["rf-below-minimum"]


def test_under_replicated_and_under_min_isr_are_distinct():
    t = topic(min_isr=2)
    t.partitions[0] = Partition(0, 1, (1, 2, 3), (1, 2))  # one replica out of sync
    t.partitions[1] = Partition(1, 2, (2, 3, 1), (2,))  # below min.insync.replicas
    findings = audit(Cluster([t]))
    by_rule = {f.rule: f for f in findings}
    assert by_rule["under-replicated"].partitions == [0]
    assert by_rule["under-min-isr"].partitions == [1]
    assert by_rule["under-min-isr"].severity == "critical"
    assert findings[0].rule == "under-min-isr"  # worst first


def test_offline_partition():
    t = topic(min_isr=2)
    t.partitions[2] = Partition(2, None, (3, 1, 2), ())
    findings = audit(Cluster([t]))
    assert findings[0].rule == "offline-partitions" and findings[0].partitions == [2]
    assert "under-min-isr" not in rules_of(findings)


def test_unclean_leader_election():
    t = topic(min_isr=2, configs={"unclean.leader.election.enable": "true"})
    assert rules_of(audit(Cluster([t]))) == ["unclean-leader-election"]
    t.configs["unclean.leader.election.enable"] = "false"
    assert audit(Cluster([t])) == []


def test_leader_skew_and_preferred_leader():
    t = topic(min_isr=2, parts=9, leader=1)  # broker 1 leads everything
    findings = audit(Cluster([t]))
    by_rule = {f.rule: f for f in findings}
    assert by_rule["leader-skew"].brokers == [1, 2, 3]
    assert by_rule["leader-skew"].topic is None
    assert "6 partitions of 9 are not led" in by_rule["not-preferred-leader"].message
    assert "replica-skew" not in by_rule


def test_most_even_distribution_possible_is_not_skew():
    # 4 partitions over 3 brokers can at best be led 2/1/1.
    assert audit(Cluster([topic(min_isr=2, parts=4)])) == []


def test_replica_skew_counts_brokers_that_host_nothing():
    cluster = Cluster([topic(min_isr=2, parts=6)], brokers=[1, 2, 3, 4])
    findings = audit(cluster)
    replica = next(f for f in findings if f.rule == "replica-skew")
    assert 4 in replica.brokers and "broker 4 hosts 0" in replica.message
    assert audit(cluster, Rules(skew_pct=100)) == []


def test_skew_is_skipped_on_a_single_broker():
    assert audit(Cluster([topic(rf=1, parts=5, min_isr=1)]), Rules(min_rf=1, min_isr=1)) == []


def test_rules_exclude_disable_severity_and_overrides():
    cluster = Cluster([topic("logs.app", rf=2, min_isr=1), topic("__internal", rf=1, parts=1), topic("orders", rf=2, min_isr=1)])
    rules = Rules(
        exclude_topics=["__*"],
        severity={"rf-below-minimum": "critical"},
        overrides=[{"topics": ["logs.*"], "min_rf": 2, "min_isr": 1}],
        disable={"replica-skew", "leader-skew"},
    )
    findings = audit(cluster, rules)
    assert [(f.topic, f.rule, f.severity) for f in findings] == [("orders", "rf-below-minimum", "critical")]


def test_load_rules_file_and_reject_typos():
    rules = load_rules('{"min_rf": 2, "disable": ["leader-skew"], "overrides": [{"topics": "tmp-*", "min_isr": 1}]}')
    assert rules.min_rf == 2 and not rules.enabled("leader-skew")
    assert rules.for_topic("tmp-1").min_isr == 1 and rules.for_topic("orders").min_isr == 2
    for bad in ('{"min_replication": 3}', '{"disable": ["no-such-rule"]}', '{"severity": {"leader-skew": "fatal"}}',
                '{"overrides": [{"min_rf": 2}]}', "[1, 2]", "{oops"):
        with pytest.raises(RulesError):
            load_rules(bad)
