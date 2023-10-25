"""The audit itself: pure functions from a Cluster and Rules to a list of findings."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field

from .model import Cluster, Topic
from .rules import SEVERITIES, Rules


@dataclass
class Finding:
    rule: str
    severity: str
    topic: str | None  # None for cluster-wide findings
    message: str
    fix: str = ""
    partitions: list[int] = field(default_factory=list)
    brokers: list[int] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _ids(values: list[int], limit: int = 8) -> str:
    shown = ", ".join(str(v) for v in values[:limit])
    return shown + (f" and {len(values) - limit} more" if len(values) > limit else "")


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def _verb(items: list, singular: str, plural: str) -> str:
    return singular if len(items) == 1 else plural


def effective_min_isr(topic: Topic, rules: Rules) -> int:
    value = topic.configs.get("min.insync.replicas")
    return int(value) if value not in (None, "") else rules.default_min_isr


def audit_topic(topic: Topic, rules: Rules) -> list[Finding]:
    """Check one topic. `rules` should already be resolved with `Rules.for_topic`."""
    found: list[Finding] = []

    def add(rule: str, message: str, fix: str = "", partitions: list[int] | None = None) -> None:
        if rules.enabled(rule):
            found.append(Finding(rule, rules.severity_of(rule), topic.name, message, fix, partitions or []))

    rf = topic.replication_factor
    min_isr = effective_min_isr(topic, rules)

    offline = [p.id for p in topic.partitions if p.leader is None]
    if offline:
        add("offline-partitions",
            f"{_plural(len(offline), 'partition')} of {len(topic.partitions)} {_verb(offline, 'has', 'have')} no leader "
            f"(partition {_ids(offline)}): nothing can be produced to or consumed from "
            f"{_verb(offline, 'it', 'them')}.",
            "Bring the brokers that host their replicas back online.", offline)

    # When min.insync.replicas exceeds the replication factor the setting itself is the problem.
    under_min = [p.id for p in topic.partitions
                 if p.leader is not None and len(p.isr) < min_isr and min_isr <= rf]
    if under_min:
        add("under-min-isr",
            f"{_plural(len(under_min), 'partition')} {_verb(under_min, 'has', 'have')} fewer in-sync replicas than min.insync.replicas={min_isr} "
            f"(partition {_ids(under_min)}): producers using acks=all are rejected with NotEnoughReplicas.",
            "Restore the missing replicas; do not lower min.insync.replicas to work around it.", under_min)

    under_replicated = [p.id for p in topic.partitions
                        if p.leader is not None and len(p.isr) < len(p.replicas) and p.id not in under_min]
    if under_replicated:
        add("under-replicated",
            f"{_plural(len(under_replicated), 'partition')} of {len(topic.partitions)} "
            f"{_verb(under_replicated, 'is', 'are')} under-replicated "
            f"(partition {_ids(under_replicated)}): some assigned replicas are not in the ISR.",
            "Check the brokers missing from the ISR: down, slow disks, or replication lagging behind.",
            under_replicated)

    if topic.partitions and rf < rules.min_rf:
        consequence = ("a single broker failure makes the topic unavailable and can lose its data"
                       if rf == 1 else f"it tolerates the loss of only {_plural(rf - 1, 'broker')}")
        add("rf-below-minimum",
            f"Replication factor is {rf}, below the required minimum of {rules.min_rf}: {consequence}.",
            f"Increase to {rules.min_rf} replicas with kafka-reassign-partitions.")

    if topic.partitions:
        if min_isr > rf:
            add("min-isr-above-rf",
                f"min.insync.replicas={min_isr} is greater than the replication factor {rf}: "
                "produce requests with acks=all can never succeed.",
                f"Set min.insync.replicas to at most {max(rf - 1, 1)}, or add replicas.")
        elif min_isr == rf and rf > 1:
            add("min-isr-no-headroom",
                f"min.insync.replicas={min_isr} equals the replication factor {rf}: one broker down or one "
                "replica falling out of the ISR blocks every producer that uses acks=all.",
                f"Set min.insync.replicas={rf - 1}, or raise the replication factor to {rf + 1}.")
        # A target that the replication factor cannot satisfy is reported by rf-below-minimum instead.
        target = min(rules.min_isr, rf - 1)
        if rf > 1 and min_isr < target:
            add("min-isr-too-low",
                f"min.insync.replicas={min_isr} with replication factor {rf} (required: at least {rules.min_isr}): "
                f"an acks=all write is acknowledged once {_plural(min_isr, 'replica')} "
                f"{'has' if min_isr == 1 else 'have'} it, so losing "
                f"{'that broker' if min_isr == 1 else 'those brokers'} can lose acknowledged records.",
                f"kafka-configs --alter --entity-type topics --entity-name {topic.name} "
                f"--add-config min.insync.replicas={target}. It only protects producers that use acks=all.")

    if str(topic.configs.get("unclean.leader.election.enable", "")).lower() == "true":
        add("unclean-leader-election",
            "unclean.leader.election.enable=true: an out-of-sync replica may become leader, "
            "silently discarding records the previous leader had acknowledged.",
            "Set unclean.leader.election.enable=false unless availability matters more than the data.")

    return found


def _skewed(counts: dict[int, int], pct: float) -> tuple[list[int], list[int], float]:
    """Brokers above and below the allowed band around the per-broker average.

    The band is widened to whole numbers so the most even distribution possible
    (for example 2, 1, 1) is never reported.
    """
    mean = sum(counts.values()) / len(counts)
    upper = math.ceil(mean * (1 + pct / 100))
    lower = math.floor(mean * (1 - pct / 100))
    high = sorted(b for b, c in counts.items() if c > upper)
    low = sorted(b for b, c in counts.items() if c < lower)
    return high, low, mean


def audit_balance(cluster: Cluster, rules: Rules) -> list[Finding]:
    """Cluster-wide checks: how leaders and replicas are spread over the brokers."""
    found: list[Finding] = []
    brokers = cluster.broker_ids()
    leaders = dict.fromkeys(brokers, 0)
    replicas = dict.fromkeys(brokers, 0)
    not_preferred: dict[str, int] = {}
    total = 0
    for topic in cluster.topics:
        for p in topic.partitions:
            total += 1
            for broker in p.replicas:
                replicas[broker] += 1
            if p.leader is not None:
                leaders[p.leader] = leaders.get(p.leader, 0) + 1
                if p.replicas and p.leader != p.replicas[0]:
                    not_preferred[topic.name] = not_preferred.get(topic.name, 0) + 1

    def describe(counts: dict[int, int]) -> str:
        return ", ".join(f"broker {b}: {c}" for b, c in sorted(counts.items()))

    if len(brokers) > 1 and total:
        checks = (
            ("leader-skew", leaders, "Partition leaders", "leads",
             "Run a preferred leader election (kafka-leader-election --election-type preferred "
             "--all-topic-partitions); if it stays uneven, reassign replicas."),
            ("replica-skew", replicas, "Replicas", "hosts",
             "Rebalance with kafka-reassign-partitions (or Cruise Control) so disk and network load even out."),
        )
        for rule, counts, what, verb, fix in checks:
            if not rules.enabled(rule):
                continue
            high, low, mean = _skewed(counts, rules.skew_pct)
            if high or low:
                parts = [f"broker {b} {verb} {counts[b]}" for b in high + low]
                found.append(Finding(
                    rule, rules.severity_of(rule), None,
                    f"{what} are unevenly spread: {', '.join(parts)}, against an average of {mean:.1f} per broker "
                    f"(allowed deviation {rules.skew_pct:g}%). All brokers: {describe(counts)}.",
                    fix, brokers=high + low))

    if not_preferred and rules.enabled("not-preferred-leader"):
        count = sum(not_preferred.values())
        worst = sorted(not_preferred, key=lambda name: (-not_preferred[name], name))[:5]
        found.append(Finding(
            "not-preferred-leader", rules.severity_of("not-preferred-leader"), None,
            f"{_plural(count, 'partition')} of {total} {'is' if count == 1 else 'are'} not led by their preferred (first listed) replica, "
            f"mostly in: {', '.join(worst)}. This usually follows a broker restart.",
            "kafka-leader-election --election-type preferred --all-topic-partitions "
            "(automatic when auto.leader.rebalance.enable=true)."))
    return found


def audit(cluster: Cluster, rules: Rules | None = None) -> list[Finding]:
    """Run every enabled rule and return findings, worst first."""
    rules = rules or Rules()
    found: list[Finding] = []
    for topic in cluster.topics:
        if not rules.excluded(topic.name):
            found += audit_topic(topic, rules.for_topic(topic.name))
    found += audit_balance(cluster, rules)
    found.sort(key=lambda f: (-SEVERITIES.index(f.severity), f.topic is None, f.topic or "", f.rule))
    return found
