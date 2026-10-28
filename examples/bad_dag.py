"""Nightly revenue rollup - deliberately full of the mistakes dag-critic looks for.

This file is never imported by Airflow in this repository; it is parsed as text by the critic
and by the tests. The password below is a dummy value, not a real credential.
"""

import datetime

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

REVENUE_URL = "https://internal.example.com/revenue"

client = boto3.client("s3", region_name="eu-west-1")

with DAG(
    dag_id="revenue_rollup",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=True,
) as dag:

    def fetch():
        return requests.get(REVENUE_URL, timeout=5).json()

    pull = PythonOperator(task_id="pull", python_callable=fetch)
    pull_again = PythonOperator(task_id="pull", python_callable=fetch)
    wait = ExternalTaskSensor(task_id="wait_for_upstream")
    persist = PythonOperator(
        task_id=f"persist_{datetime.date.today()}",
        python_callable=fetch,
        password="not-a-real-password",
    )

    pull >> wait >> persist
