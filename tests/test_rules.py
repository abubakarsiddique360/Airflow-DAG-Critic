from airflow_dag_critic.dagfile import parse_source
from airflow_dag_critic.rules import review

CLEAN = '''
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

ARGS = {"owner": "data-platform", "retries": 3}

with DAG(
    dag_id="clean",
    default_args=ARGS,
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
):
    task = PythonOperator(task_id="task", python_callable=lambda: 1)
'''


def codes(source: str) -> list[str]:
    return [finding.code for finding in review(parse_source(source)).findings]


def test_clean_dag_has_no_findings():
    result = review(parse_source(CLEAN))
    assert result.clean
    assert result.grade == "A"
    assert result.risk == 0


def test_reports_counts_and_counts_of_counts():
    result = review(parse_source(CLEAN))
    assert result.dag_count == 1
    assert result.task_count == 1
    assert result.counts == {"critical": 0, "high": 0, "medium": 0, "low": 0}


def test_no_dag_found():
    assert "no-dag" in codes("import os\n")


def test_catchup_default_and_enabled():
    default = codes(
        'from airflow import DAG\nDAG(dag_id="x", start_date=1, schedule="0 1 * * *")\n'
    )
    assert "catchup-default" in default
    enabled = codes(
        'from airflow import DAG\nDAG(dag_id="x", start_date=1, schedule="@daily", catchup=True)\n'
    )
    assert "catchup-enabled" in enabled


def test_missing_start_date_and_schedule_and_owner():
    found = codes('from airflow import DAG\nDAG(dag_id="x", catchup=False)\n')
    assert "no-start-date" in found
    assert "no-schedule" in found
    assert "no-owner" in found


def test_sensor_without_timeout_is_high():
    source = '''
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
ExternalTaskSensor(task_id="wait")
'''
    result = review(parse_source(source))
    severity = {item.code: item.severity for item in result.findings}
    assert severity["sensor-without-timeout"] == "high"


def test_sensor_with_timeout_is_clean():
    source = '''
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
ExternalTaskSensor(task_id="wait", timeout=60)
'''
    assert "sensor-without-timeout" not in codes(source)


def test_hardcoded_secret_is_critical():
    source = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
PythonOperator(task_id="t", python_callable=print, password="hunter2")
'''
    result = review(parse_source(source))
    critical = [item for item in result.findings if item.severity == "critical"]
    assert len(critical) == 1
    assert critical[0].code == "hardcoded-secret"
    assert "hunter2" not in critical[0].message


def test_empty_secret_literal_is_not_flagged():
    source = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
PythonOperator(task_id="t", python_callable=print, password="")
'''
    assert "hardcoded-secret" not in codes(source)


def test_duplicate_task_id():
    source = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
PythonOperator(task_id="same", python_callable=print)
PythonOperator(task_id="same", python_callable=print)
'''
    assert "duplicate-task-id" in codes(source)


def test_dynamic_ids_are_not_duplicates():
    source = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
PythonOperator(task_id=f"a_{1}", python_callable=print)
PythonOperator(task_id=f"b_{1}", python_callable=print)
'''
    found = codes(source)
    assert "duplicate-task-id" not in found
    assert found.count("dynamic-task-id") == 2


def test_module_level_side_effect():
    source = '''
import boto3
from airflow import DAG
client = boto3.client("s3")
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
'''
    assert "module-level-side-effect" in codes(source)


def test_no_retries_is_aggregated():
    source = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
DAG(dag_id="x", start_date=1, schedule="@daily", catchup=False)
PythonOperator(task_id="a", python_callable=print)
PythonOperator(task_id="b", python_callable=print)
'''
    result = review(parse_source(source))
    retries = [item for item in result.findings if item.code == "no-retries"]
    assert len(retries) == 1
    assert retries[0].scope == "2 task(s)"


def test_findings_are_sorted_by_severity():
    from airflow_dag_critic.rules import SEVERITY_ORDER

    result = review(
        parse_source(
            '''
import boto3
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
client = boto3.client("s3")
ExternalTaskSensor(task_id="wait")
DAG(dag_id="x", catchup=True)
'''
        )
    )
    order = [SEVERITY_ORDER[item.severity] for item in result.findings]
    assert order == sorted(order)


def test_fail_on_threshold():
    result = review(parse_source("import os\n"))
    assert result.should_fail("high")
    assert not result.should_fail("never")


def test_grade_boundaries():
    result = review(parse_source("import os\n"))
    assert result.grade in ("C", "D", "F")
