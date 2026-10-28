import json

import pytest

from airflow_dag_critic.cli import main
from airflow_dag_critic.dagfile import parse_source
from airflow_dag_critic.report import (
    render_catalogue,
    render_json,
    render_markdown,
    render_mermaid,
    render_review,
    render_summary,
)
from airflow_dag_critic.rules import review

BAD = '''
import boto3
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
client = boto3.client("s3")
with DAG(dag_id="bad", start_date=1, schedule="@daily", catchup=True):
    wait = ExternalTaskSensor(task_id="wait")
'''

GOOD = '''
import datetime
from airflow import DAG
ARGS = {"owner": "data", "retries": 3}
with DAG(dag_id="good", default_args=ARGS, start_date=datetime.datetime(2024, 1, 1),
         schedule="@daily", catchup=False):
    pass
'''


@pytest.fixture
def files(tmp_path):
    bad = tmp_path / "bad_dag.py"
    bad.write_text(BAD, encoding="utf-8")
    good = tmp_path / "good_dag.py"
    good.write_text(GOOD, encoding="utf-8")
    return bad, good


def test_review_text_shows_grades(tmp_path, files, capsys):
    bad, _ = files
    assert main(["review", str(bad), "--fail-on", "never"]) == 0
    output = capsys.readouterr().out
    assert "DAG review" in output
    assert "CRITICAL" in output or "HIGH" in output


def test_review_exit_code_on_high(files, capsys):
    bad, _ = files
    assert main(["review", str(bad)]) == 1
    capsys.readouterr()


def test_review_clean_file_exits_zero(files, capsys):
    _, good = files
    assert main(["review", str(good)]) == 0
    assert "No findings" in capsys.readouterr().out


def test_review_markdown(files, capsys):
    bad, _ = files
    assert main(["review", str(bad), "--format", "markdown", "--fail-on", "never"]) == 0
    output = capsys.readouterr().out
    assert "| Severity | Line | Scope | Finding | Fix |" in output


def test_review_json(files, capsys):
    bad, _ = files
    assert main(["review", str(bad), "--format", "json", "--fail-on", "never"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["dags"] == 1
    assert payload[0]["findings"]


def test_review_summary(files, capsys):
    bad, good = files
    assert main(["review", str(bad), str(good), "--summary", "--fail-on", "never"]) == 0
    output = capsys.readouterr().out
    assert "Summary" in output
    assert "bad_dag.py" in output and "good_dag.py" in output


def test_review_missing_file(tmp_path, capsys):
    assert main(["review", str(tmp_path / "nope.py")]) == 2
    assert "file not found" in capsys.readouterr().err


def test_review_syntax_error(tmp_path, capsys):
    broken = tmp_path / "broken.py"
    broken.write_text("def (:\n", encoding="utf-8")
    assert main(["review", str(broken)]) == 2
    assert "error:" in capsys.readouterr().err


def test_graph_command(files, capsys):
    bad, _ = files
    assert main(["graph", str(bad)]) == 0
    output = capsys.readouterr().out
    assert "flowchart LR" in output
    assert "wait" in output


def test_graph_missing_file(tmp_path, capsys):
    assert main(["graph", str(tmp_path / "nope.py")]) == 2


def test_stats_command(files, capsys):
    bad, good = files
    assert main(["stats", str(bad), str(good)]) == 0
    output = capsys.readouterr().out
    assert "dags 1 tasks 1" in output


def test_stats_missing_file(tmp_path, capsys):
    assert main(["stats", str(tmp_path / "nope.py")]) == 2


def test_rules_command(capsys):
    assert main(["rules"]) == 0
    output = capsys.readouterr().out
    assert "hardcoded-secret" in output
    assert "sensor-without-timeout" in output


def test_catalogue_mentions_every_severity():
    text = render_catalogue()
    for severity in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        assert severity in text


def test_markdown_for_clean_review():
    text = render_markdown(review(parse_source(GOOD)))
    assert "No findings" in text


def test_json_serialises_findings():
    payload = json.loads(render_json(review(parse_source(BAD))))
    assert payload["counts"]["high"] >= 1
    assert isinstance(payload["findings"][0]["points"], int)


def test_summary_totals():
    results = [review(parse_source(BAD)), review(parse_source(GOOD))]
    text = render_summary(results)
    assert "2 file(s)" in text
    assert "combined risk" in text


def test_mermaid_handles_empty_file():
    text = render_mermaid(parse_source("import os\n"))
    assert "no operators found" in text


def test_review_text_lists_fix_hints():
    text = render_review(review(parse_source(BAD)))
    assert "fix:" in text
