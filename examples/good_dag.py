"""The same pipeline with every finding from bad_dag.py fixed.

Kept as a plain file: dag-critic parses it, it is not meant to be run by a scheduler here.
"""

import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

REVENUE_URL = "https://internal.example.com/revenue"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "email_on_failure": True,
}


def fetch_revenue(**context):
    response = requests.get(REVENUE_URL, timeout=5)
    response.raise_for_status()
    return response.json()


def persist_revenue(**context):
    payload = context["ti"].xcom_pull(task_ids="pull")
    return len(payload)


with DAG(
    dag_id="revenue_rollup",
    default_args=DEFAULT_ARGS,
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    pull = PythonOperator(task_id="pull", python_callable=fetch_revenue)
    wait = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_ingest",
        timeout=900,
        mode="reschedule",
    )
    persist = PythonOperator(task_id="persist", python_callable=persist_revenue)

    pull >> wait >> persist
