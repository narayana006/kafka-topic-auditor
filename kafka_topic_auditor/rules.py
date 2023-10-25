"""Audit thresholds and the rules file."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from fnmatch import fnmatchcase

SEVERITIES = ("info", "warning", "critical")

# rule id -> default severity
RULES = {
    "offline-partitions": "critical",
    "under-min-isr": "critical",
    "min-isr-above-rf": "critical",
    "under-replicated": "warning",
    "rf-below-minimum": "warning",
    "min-isr-too-low": "warning",
    "min-isr-no-headroom": "warning",
    "unclean-leader-election": "warning",
    "leader-skew": "warning",
    "replica-skew": "warning",
    "not-preferred-leader": "info",
}


class RulesError(ValueError):
    """Raised for an invalid rules file."""


@dataclass
class Rules:
    min_rf: int = 3  # smallest acceptable replication factor
    min_isr: int = 2  # smallest acceptable min.insync.replicas
    default_min_isr: int = 1  # assumed when a topic's configs do not list min.insync.replicas
    skew_pct: float = 20.0  # allowed deviation of a broker from the per-broker average
    exclude_topics: list[str] = field(default_factory=list)  # glob patterns
    disable: set[str] = field(default_factory=set)  # rule ids
    severity: dict[str, str] = field(default_factory=dict)  # rule id -> severity
    overrides: list[dict] = field(default_factory=list)  # [{"topics": [...], "min_rf": 2, "min_isr": 1}]

    def __post_init__(self) -> None:
        for rule in list(self.disable) + list(self.severity):
            if rule not in RULES:
                raise RulesError(f"unknown rule {rule!r}; known rules: {', '.join(RULES)}")
        for level in self.severity.values():
            if level not in SEVERITIES:
                raise RulesError(f"unknown severity {level!r}; use one of {', '.join(SEVERITIES)}")
        for override in self.overrides:
            unknown = set(override) - {"topics", "min_rf", "min_isr"}
            if unknown or "topics" not in override:
                raise RulesError(f"override needs 'topics' and may set min_rf and min_isr, got {override!r}")

    def enabled(self, rule: str) -> bool:
        return rule not in self.disable

    def severity_of(self, rule: str) -> str:
        return self.severity.get(rule, RULES[rule])

    def excluded(self, topic: str) -> bool:
        return any(fnmatchcase(topic, pattern) for pattern in self.exclude_topics)

    def for_topic(self, topic: str) -> "Rules":
        """Thresholds for one topic: the first matching override wins."""
        for override in self.overrides:
            patterns = override["topics"]
            patterns = [patterns] if isinstance(patterns, str) else patterns
            if any(fnmatchcase(topic, pattern) for pattern in patterns):
                return replace(self, min_rf=int(override.get("min_rf", self.min_rf)),
                               min_isr=int(override.get("min_isr", self.min_isr)))
        return self


def load_rules(text: str) -> Rules:
    """Parse a JSON rules file. Unknown keys are rejected so typos do not pass silently."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RulesError(f"rules file is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise RulesError("rules file must contain a JSON object")
    known = set(Rules.__dataclass_fields__)
    unknown = set(data) - known
    if unknown:
        raise RulesError(f"unknown rules key(s): {', '.join(sorted(unknown))}; known keys: {', '.join(sorted(known))}")
    if "disable" in data:
        data["disable"] = set(data["disable"])
    try:
        return Rules(**data)
    except TypeError as exc:
        raise RulesError(str(exc)) from exc
