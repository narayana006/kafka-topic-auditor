"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import sys
import textwrap

from .audit import Finding, audit
from .model import Cluster
from .parsers import ParseError, cluster_to_dict, load_cluster
from .rules import RULES, SEVERITIES, Rules, RulesError, load_rules


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-topic-auditor",
        description="Audit Kafka topics for risky replication and durability settings.",
        epilog="Rules: " + ", ".join(RULES),
    )
    src = p.add_argument_group("input (use --input or --bootstrap)")
    src.add_argument("--input", metavar="FILE",
                     help="output of `kafka-topics --describe`, or a JSON snapshot. Use - for stdin.")
    src.add_argument("--bootstrap", metavar="HOST:PORT", help="read a live cluster (needs confluent-kafka)")
    src.add_argument("-X", dest="client_config", action="append", default=[], metavar="KEY=VALUE",
                     help="extra client property for --bootstrap, e.g. -X security.protocol=SASL_SSL")
    src.add_argument("--brokers", metavar="IDS",
                     help="comma-separated ids of all brokers, so brokers that host nothing count in the skew "
                          "checks (text input does not list them)")
    src.add_argument("--save-snapshot", metavar="FILE", help="write what was read as a JSON snapshot")
    rules = p.add_argument_group("rules (command line values override the rules file)")
    rules.add_argument("--rules", metavar="FILE", help="JSON rules file")
    rules.add_argument("--min-rf", type=int, help="minimum replication factor (default 3)")
    rules.add_argument("--min-isr", type=int, help="minimum min.insync.replicas (default 2)")
    rules.add_argument("--default-min-isr", type=int,
                       help="min.insync.replicas assumed for topics whose configs do not list it (default 1)")
    rules.add_argument("--skew-pct", type=float,
                       help="allowed deviation of a broker's leader or replica count from the average (default 20)")
    rules.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                       help="skip topics matching this pattern in the per-topic rules; repeatable")
    rules.add_argument("--disable", action="append", default=[], metavar="RULE", help="turn a rule off; repeatable")
    out = p.add_argument_group("output")
    out.add_argument("--json", action="store_true", help="print findings as JSON")
    out.add_argument("--fail-on", choices=["info", "warning", "critical", "never"], default="warning",
                     help="exit with status 1 if a finding of this severity or worse exists (default warning)")
    return p


def _summary(cluster: Cluster, findings: list[Finding]) -> dict:
    counts = {level: sum(1 for f in findings if f.severity == level) for level in reversed(SEVERITIES)}
    return {
        "topics": len(cluster.topics),
        "partitions": sum(len(t.partitions) for t in cluster.topics),
        "brokers": len(cluster.broker_ids()),
        "findings": len(findings),
        **counts,
    }


def render_text(cluster: Cluster, findings: list[Finding], width: int = 100) -> str:
    s = _summary(cluster, findings)
    def n(key: str) -> str:
        return f"{s[key]} {key if s[key] != 1 else key[:-1]}"

    out = [f"Audited {n('topics')}, {n('partitions')} on {n('brokers')}."]
    if not findings:
        out.append("No findings.")
        return "\n".join(out) + "\n"
    out.append(f"{n('findings')}: {s['critical']} critical, {s['warning']} warning, {s['info']} info.")
    for f in findings:
        out.append("")
        out.append(f"{f.severity.upper():<8}  {f.topic or '(cluster)'}  [{f.rule}]")
        out += textwrap.wrap(f.message, width, initial_indent="  ", subsequent_indent="  ")
        if f.fix:
            out += textwrap.wrap("Fix: " + f.fix, width, initial_indent="  ", subsequent_indent="       ")
    return "\n".join(out) + "\n"


def _build_rules(args: argparse.Namespace) -> Rules:
    if args.rules:
        with open(args.rules, encoding="utf-8") as handle:
            rules = load_rules(handle.read())
    else:
        rules = Rules()
    for name in ("min_rf", "min_isr", "default_min_isr", "skew_pct"):
        value = getattr(args, name)
        if value is not None:
            setattr(rules, name, value)
    rules.exclude_topics = list(rules.exclude_topics) + args.exclude
    rules.disable = set(rules.disable) | set(args.disable)
    rules.__post_init__()  # validate rule names given on the command line
    return rules


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if bool(args.input) == bool(args.bootstrap):
        parser.error("provide either --input FILE or --bootstrap HOST:PORT")

    try:
        rules = _build_rules(args)
        if args.input:
            text = sys.stdin.read() if args.input == "-" else open(args.input, encoding="utf-8").read()
            cluster = load_cluster(text)
        else:
            from .live import fetch_cluster

            cluster = fetch_cluster(args.bootstrap, dict(item.split("=", 1) for item in args.client_config))
        if args.brokers:
            cluster.brokers = sorted(set(cluster.brokers) | {int(b) for b in args.brokers.split(",") if b.strip()})
    except (ParseError, RulesError, OSError, ValueError) as exc:
        print(f"kafka-topic-auditor: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # live adapter errors: unreachable broker, missing package
        if args.input:
            raise
        print(f"kafka-topic-auditor: {exc}", file=sys.stderr)
        return 2

    if args.save_snapshot:
        with open(args.save_snapshot, "w", encoding="utf-8") as handle:
            json.dump(cluster_to_dict(cluster), handle, indent=2)
            handle.write("\n")

    findings = audit(cluster, rules)
    if args.json:
        print(json.dumps({"summary": _summary(cluster, findings),
                          "findings": [f.to_dict() for f in findings]}, indent=2))
    else:
        sys.stdout.write(render_text(cluster, findings))

    if args.fail_on != "never" and any(
        SEVERITIES.index(f.severity) >= SEVERITIES.index(args.fail_on) for f in findings
    ):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
