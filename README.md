# kafka-topic-auditor

Audit Kafka topics for settings and states that cost you data or availability when a broker fails: low replication factor, weak `min.insync.replicas`, under-replicated and offline partitions, and leaders or replicas piled onto a few brokers.

**Status: early but usable (v0.1.0).** Eleven rules covering replication factor, `min.insync.replicas`, partition health and broker balance work today, with a rules file, JSON output and an exit code for CI. Input can be saved `kafka-topics --describe` output, a JSON snapshot, or a live cluster. Finding unused topics (no traffic, no consumers) is not built yet.

## Install

Python 3.10 or newer. The core has no dependencies.

```bash
git clone https://github.com/narayana006/kafka-topic-auditor.git
cd kafka-topic-auditor
pip install .            # audit saved output and snapshots
pip install ".[live]"    # also installs confluent-kafka to read a live cluster
```

## Quick start (no cluster needed)

`examples/topics-describe.txt` is `kafka-topics --describe` output from a four-broker cluster with several problems in it.

```bash
kafka-topic-auditor --input examples/topics-describe.txt --min-rf 3
```

```text
Audited 7 topics, 28 partitions on 4 brokers.
11 findings: 2 critical, 8 warning, 1 info.

CRITICAL  inventory  [under-min-isr]
  1 partition has fewer in-sync replicas than min.insync.replicas=2 (partition 1): producers using
  acks=all are rejected with NotEnoughReplicas.
  Fix: Restore the missing replicas; do not lower min.insync.replicas to work around it.

CRITICAL  legacy-export  [offline-partitions]
  1 partition of 2 has no leader (partition 0): nothing can be produced to or consumed from it.
  Fix: Bring the brokers that host their replicas back online.

WARNING   audit-log  [min-isr-no-headroom]
  min.insync.replicas=3 equals the replication factor 3: one broker down or one replica falling out
  of the ISR blocks every producer that uses acks=all.
  Fix: Set min.insync.replicas=2, or raise the replication factor to 4.

WARNING   clickstream  [rf-below-minimum]
  Replication factor is 2, below the required minimum of 3: it tolerates the loss of only 1 broker.
  Fix: Increase to 3 replicas with kafka-reassign-partitions.

WARNING   legacy-export  [rf-below-minimum]
  Replication factor is 1, below the required minimum of 3: a single broker failure makes the topic
  unavailable and can lose its data.
  Fix: Increase to 3 replicas with kafka-reassign-partitions.

WARNING   payments  [under-replicated]
  1 partition of 4 is under-replicated (partition 2): some assigned replicas are not in the ISR.
  Fix: Check the brokers missing from the ISR: down, slow disks, or replication lagging behind.

WARNING   user-profiles  [min-isr-too-low]
  min.insync.replicas=1 with replication factor 3 (required: at least 2): an acks=all write is
  acknowledged once 1 replica has it, so losing that broker can lose acknowledged records.
  Fix: kafka-configs --alter --entity-type topics --entity-name user-profiles --add-config
       min.insync.replicas=2. It only protects producers that use acks=all.

WARNING   user-profiles  [unclean-leader-election]
  unclean.leader.election.enable=true: an out-of-sync replica may become leader, silently discarding
  records the previous leader had acknowledged.
  Fix: Set unclean.leader.election.enable=false unless availability matters more than the data.

WARNING   (cluster)  [leader-skew]
  Partition leaders are unevenly spread: broker 1 leads 15, broker 4 leads 0, against an average of
  6.8 per broker (allowed deviation 20%). All brokers: broker 1: 15, broker 2: 7, broker 3: 5,
  broker 4: 0.
  Fix: Run a preferred leader election (kafka-leader-election --election-type preferred --all-topic-
       partitions); if it stays uneven, reassign replicas.

WARNING   (cluster)  [replica-skew]
  Replicas are unevenly spread: broker 1 hosts 25, broker 2 hosts 24, broker 3 hosts 24, broker 4
  hosts 1, against an average of 18.5 per broker (allowed deviation 20%). All brokers: broker 1: 25,
  broker 2: 24, broker 3: 24, broker 4: 1.
  Fix: Rebalance with kafka-reassign-partitions (or Cruise Control) so disk and network load even
       out.

INFO      (cluster)  [not-preferred-leader]
  2 partitions of 28 are not led by their preferred (first listed) replica, mostly in: inventory,
  payments. This usually follows a broker restart.
  Fix: kafka-leader-election --election-type preferred --all-topic-partitions (automatic when
       auto.leader.rebalance.enable=true).
```

The command exits with status 1 because there are findings at `warning` or above.

## Usage

### Audit saved `kafka-topics` output

```bash
kafka-topics.sh --bootstrap-server broker:9092 --describe > topics.txt
kafka-topic-auditor --input topics.txt
# or in one go
kafka-topics.sh --bootstrap-server broker:9092 --describe | kafka-topic-auditor --input -
```

Two things to know about text input:

- `kafka-topics --describe` lists only the configs that differ from Kafka's built-in defaults. When `min.insync.replicas` is not listed for a topic, the auditor assumes Kafka's default of 1. Change that assumption with `--default-min-isr`.
- The output names only brokers that host a replica. Pass `--brokers 1,2,3,4,5` to include brokers that host nothing (for example a freshly added one) in the skew checks.

### Audit a live cluster

```bash
kafka-topic-auditor --bootstrap broker:9092 --min-rf 3
```

Reads cluster metadata and topic configs with read-only Admin API calls. Here `min.insync.replicas` is the effective value reported by the broker, including broker-level defaults. Pass client properties with `-X`, for example `-X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...`. Add `--save-snapshot cluster.json` to keep what was read; a snapshot can be audited again later with `--input cluster.json`.

### Rules

| Rule | Default severity | Triggers when |
| --- | --- | --- |
| `offline-partitions` | critical | A partition has no leader. |
| `under-min-isr` | critical | A partition's ISR is smaller than `min.insync.replicas`; `acks=all` producers get `NotEnoughReplicas`. |
| `min-isr-above-rf` | critical | `min.insync.replicas` is greater than the replication factor; `acks=all` writes can never succeed. |
| `under-replicated` | warning | A partition's ISR is smaller than its replica list (and not already under min ISR). |
| `rf-below-minimum` | warning | Replication factor is below `min_rf` (default 3). |
| `min-isr-too-low` | warning | `min.insync.replicas` is below `min_isr` (default 2), so an `acks=all` write can be acknowledged by a single replica. Not raised when the replication factor could not satisfy the target with one replica to spare; `rf-below-minimum` covers that. |
| `min-isr-no-headroom` | warning | `min.insync.replicas` equals the replication factor (above 1): one replica out of sync blocks `acks=all` producers. |
| `unclean-leader-election` | warning | The topic has `unclean.leader.election.enable=true`. |
| `leader-skew` | warning | A broker leads more or fewer partitions than the per-broker average allows (`skew_pct`, default 20%). |
| `replica-skew` | warning | Same, for the number of replicas a broker hosts. |
| `not-preferred-leader` | info | Partitions whose leader is not the first replica in their assignment. |

The skew band is rounded outwards to whole partitions, so the most even spread possible (say 2, 1, 1) is never reported. Skew is computed over the whole cluster, including excluded topics.

Keep in mind that `min.insync.replicas` only takes effect for producers that send with `acks=all`. The auditor checks the topic side; it cannot see producer settings.

### Rules file

Thresholds, exclusions and severities can live in a JSON file kept next to your infrastructure code:

```json
{
  "min_rf": 3,
  "min_isr": 2,
  "skew_pct": 25,
  "exclude_topics": [
    "__*"
  ],
  "disable": [
    "not-preferred-leader"
  ],
  "severity": {
    "rf-below-minimum": "critical"
  },
  "overrides": [
    {
      "topics": [
        "clickstream",
        "logs.*"
      ],
      "min_rf": 2,
      "min_isr": 1
    }
  ]
}
```

```bash
kafka-topic-auditor --input examples/topics-describe.txt --rules examples/rules.json
```

| Key | Meaning |
| --- | --- |
| `min_rf`, `min_isr`, `default_min_isr`, `skew_pct` | Thresholds, same as the command line options. |
| `exclude_topics` | Glob patterns of topics to skip in the per-topic rules. |
| `disable` | Rule ids to turn off. |
| `severity` | Rule id to `info`, `warning` or `critical`. |
| `overrides` | Per-topic `min_rf` / `min_isr`. `topics` is a glob or a list of globs; the first matching entry wins. |

Unknown keys and rule names are rejected, so a typo cannot silently disable a check. `--min-rf`, `--min-isr`, `--default-min-isr` and `--skew-pct` on the command line override the file; `--exclude` and `--disable` add to it.

### Output and exit codes

`--json` prints `{"summary": {...}, "findings": [...]}`; each finding has `rule`, `severity`, `topic` (`null` for cluster-wide findings), `message`, `fix`, `partitions` and `brokers`.

| Exit status | Meaning |
| --- | --- |
| 0 | No finding at or above `--fail-on` (default `warning`). |
| 1 | At least one such finding. Use `--fail-on critical`, `info` or `never` to change the bar. |
| 2 | Bad input, bad rules file, or the cluster could not be read. |

### JSON snapshot format

```json
{
  "brokers": [1, 2, 3],
  "topics": [
    {
      "name": "orders",
      "configs": {"min.insync.replicas": "2"},
      "partitions": [
        {"partition": 0, "leader": 1, "replicas": [1, 2, 3], "isr": [1, 2, 3]}
      ]
    }
  ]
}
```

`leader` is `null` for an offline partition. `brokers` is optional.

### As a library

```python
from kafka_topic_auditor import Rules, audit
from kafka_topic_auditor.parsers import load_cluster

cluster = load_cluster(open("examples/topics-describe.txt").read())
for finding in audit(cluster, Rules(min_rf=3)):
    print(finding.severity, finding.topic, finding.rule)
```

## Features

- [x] Replication factor below your minimum
- [x] `min.insync.replicas` that allows data loss, leaves no headroom, or exceeds the replication factor
- [x] Under-replicated, under-min-ISR and offline partitions
- [x] Partition (replica) and leader skew across brokers
- [x] Unclean leader election enabled
- [x] Rules you can configure, with JSON output and an exit code for CI
- [x] Input from `kafka-topics --describe` text, a JSON snapshot, or a live cluster
- [ ] Topics with no traffic or no consumers

## Limitations

- **Point in time.** Under-replicated and offline partitions are judged from one reading. A broker restarting during the read looks the same as a broker that has been down for a day.
- **Producer settings are invisible.** A topic with `min.insync.replicas=2` is still unsafe for a producer using `acks=1`.
- **Broker configs are not read.** `default.replication.factor`, the broker-level `unclean.leader.election.enable` and rack placement are not checked. With text input, a broker-level `unclean.leader.election.enable=true` is only seen if `kafka-topics` lists it for the topic.
- **Skew counts partitions, not bytes or throughput.** A broker with few but very busy partitions looks fine.
- **Replication factor of a topic** is taken as the largest replica count among its partitions, which can read high during a reassignment.
- **Rules files are JSON only.**
- **Live mode** was tested against a single-node Apache Kafka 4.4.0 broker (KRaft) with confluent-kafka 2.16 over PLAINTEXT. A multi-broker or secured cluster has not been tested live; multi-broker behaviour is covered by tests on fixture data. The text parser was checked against real `kafka-topics.sh` output from Kafka 4.4.0 and against the older layout with an `Offline:` column.

## Contributing

Issues and pull requests are welcome. For a new rule, include a test in `tests/test_audit.py` showing a topic that triggers it and one that does not. Run `pip install -e ".[test]" && pytest` before sending a change.

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
