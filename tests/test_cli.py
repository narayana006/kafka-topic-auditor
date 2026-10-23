import json
from pathlib import Path

import pytest

from kafka_topic_auditor.cli import main

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
DESCRIBE = str(EXAMPLES / "topics-describe.txt")


def test_text_report_and_exit_code(capsys):
    assert main(["--input", DESCRIBE, "--min-rf", "3"]) == 1
    out = capsys.readouterr().out
    assert "Audited 7 topics, 28 partitions on 4 brokers." in out
    assert "11 findings: 2 critical, 8 warning, 1 info." in out
    assert out.index("CRITICAL") < out.index("WARNING") < out.index("INFO")


def test_json_output(capsys):
    main(["--input", DESCRIBE, "--json"])
    data = json.loads(capsys.readouterr().out)
    assert data["summary"]["critical"] == 2
    first = data["findings"][0]
    assert first == {**first, "rule": "under-min-isr", "topic": "inventory", "severity": "critical", "partitions": [1]}


def test_fail_on_levels(capsys):
    quiet = ["--input", DESCRIBE, "--exclude", "*", "--disable", "leader-skew", "--disable", "replica-skew"]
    assert main(quiet) == 0  # only the informational preferred-leader finding is left
    assert main(quiet + ["--fail-on", "info"]) == 1
    assert main(["--input", DESCRIBE, "--fail-on", "never"]) == 0


def test_rules_file_and_command_line_override(capsys):
    assert main(["--input", DESCRIBE, "--rules", str(EXAMPLES / "rules.json"), "--json"]) == 1
    found = {(f["topic"], f["rule"]): f for f in json.loads(capsys.readouterr().out)["findings"]}
    assert ("clickstream", "rf-below-minimum") not in found  # override allows RF 2
    assert found[("legacy-export", "rf-below-minimum")]["severity"] == "critical"
    assert (None, "not-preferred-leader") not in found
    main(["--input", DESCRIBE, "--rules", str(EXAMPLES / "rules.json"), "--min-rf", "1", "--json"])
    rules = {f["rule"] for f in json.loads(capsys.readouterr().out)["findings"]}
    assert "rf-below-minimum" not in rules


def test_brokers_option_adds_empty_brokers(capsys):
    main(["--input", DESCRIBE, "--brokers", "1,2,3,4,5", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert data["summary"]["brokers"] == 5
    skew = next(f for f in data["findings"] if f["rule"] == "replica-skew")
    assert 5 in skew["brokers"]


def test_snapshot_round_trip_and_stdin(tmp_path, capsys, monkeypatch):
    snapshot = tmp_path / "snap.json"
    main(["--input", DESCRIBE, "--save-snapshot", str(snapshot)])
    first = capsys.readouterr().out
    monkeypatch.setattr("sys.stdin", snapshot.open())
    main(["--input", "-"])
    assert capsys.readouterr().out == first


def test_errors_exit_2(capsys, tmp_path):
    assert main(["--input", "missing.txt"]) == 2
    bad = tmp_path / "rules.json"
    bad.write_text('{"min_rf": 3, "typo": 1}')
    assert main(["--input", DESCRIBE, "--rules", str(bad)]) == 2
    assert "typo" in capsys.readouterr().err
    assert main(["--input", DESCRIBE, "--disable", "nope"]) == 2
    with pytest.raises(SystemExit):
        main([])


def test_live_adapter_is_used_for_bootstrap(monkeypatch, capsys):
    from kafka_topic_auditor import Cluster, Partition, Topic, live

    def fake(bootstrap, config):
        assert (bootstrap, config) == ("broker:9092", {"security.protocol": "SSL"})
        return Cluster([Topic("t", [Partition(0, 1, (1, 2, 3), (1, 2, 3))], {"min.insync.replicas": "2"})], [1, 2, 3])

    monkeypatch.setattr(live, "fetch_cluster", fake)
    assert main(["--bootstrap", "broker:9092", "-X", "security.protocol=SSL"]) == 0
    assert "No findings." in capsys.readouterr().out
