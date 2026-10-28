import pytest

from airflow_dag_critic.dagfile import (
    DagParseError,
    parse_file,
    parse_source,
    value_of,
)

BASIC = '''
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="example", start_date=datetime.datetime(2024, 1, 1), schedule="0 1 * * *"):
    pull = PythonOperator(task_id="pull", python_callable=lambda: 1)
    wait = ExternalTaskSensor(task_id="wait", timeout=60)
    persist = PythonOperator(task_id="persist", python_callable=lambda: 1)
    pull >> wait >> persist
'''


def test_finds_dag_and_tasks():
    dagfile = parse_source(BASIC)
    assert [dag.dag_id for dag in dagfile.dags] == ["example"]
    assert dagfile.task_ids == ("pull", "wait", "persist")


def test_extracts_keywords():
    dag = parse_source(BASIC).dags[0]
    assert dag.get("schedule") == "'0 1 * * *'"
    assert dag.line == 7


def test_flattens_shift_chain():
    dagfile = parse_source(BASIC)
    assert dagfile.edges == (("pull", "wait"), ("wait", "persist"))


def test_edges_use_task_ids_not_variables():
    dagfile = parse_source(
        '''
from airflow.operators.python import PythonOperator
a = PythonOperator(task_id="first", python_callable=lambda: 1)
b = PythonOperator(task_id="second", python_callable=lambda: 1)
a >> b
'''
    )
    assert dagfile.edges == (("first", "second"),)


def test_branch_into_list():
    dagfile = parse_source(
        '''
from airflow.operators.python import PythonOperator
a = PythonOperator(task_id="a", python_callable=lambda: 1)
b = PythonOperator(task_id="b", python_callable=lambda: 1)
c = PythonOperator(task_id="c", python_callable=lambda: 1)
a >> [b, c]
'''
    )
    assert set(dagfile.edges) == {("a", "b"), ("a", "c")}


def test_set_downstream_style_assignment_is_not_an_edge():
    dagfile = parse_source(BASIC)
    assert ("wait", "pull") not in dagfile.edges


def test_default_args_dict_is_resolved():
    dagfile = parse_source(
        '''
from airflow import DAG
ARGS = {"owner": "data", "retries": 3}
DAG(dag_id="x", default_args=ARGS)
'''
    )
    assert dagfile.dags[0].default_args["owner"] == "'data'"
    assert dagfile.dags[0].default_args["retries"] == "3"


def test_dynamic_task_id_is_flagged():
    dagfile = parse_source(
        '''
from airflow.operators.python import PythonOperator
PythonOperator(task_id=f"x_{1}", python_callable=lambda: 1)
'''
    )
    assert dagfile.tasks[0].dynamic_id is True
    assert dagfile.tasks[0].label.startswith("PythonOperator@")


def test_module_level_side_effects_only_outside_functions():
    dagfile = parse_source(
        '''
import boto3
client = boto3.client("s3")

def inside():
    return boto3.client("s3")
'''
    )
    assert [name for name, _line in dagfile.module_calls] == ["boto3.client"]


def test_module_level_open_is_a_side_effect():
    dagfile = parse_source('with open("data.csv") as handle:\n    pass\n')
    assert dagfile.module_calls[0][0] == "open"


def test_parse_file_reads_disk(tmp_path):
    path = tmp_path / "dag.py"
    path.write_text(BASIC, encoding="utf-8")
    assert parse_file(path).task_ids == ("pull", "wait", "persist")


def test_parse_file_missing(tmp_path):
    with pytest.raises(DagParseError, match="file not found"):
        parse_file(tmp_path / "nope.py")


def test_syntax_error_is_reported_with_line():
    with pytest.raises(DagParseError, match=r"<string>:1:"):
        parse_source("def (:\n")


def test_value_of_strips_quotes():
    assert value_of("'abc'") == "abc"
    assert value_of('"abc"') == "abc"
    assert value_of("42") == "42"


def test_authoring_metadata_is_ignored():
    dagfile = parse_source(BASIC)
    assert dagfile.dags[0].dag_id == "example"
    assert len(dagfile.tasks) == 3
