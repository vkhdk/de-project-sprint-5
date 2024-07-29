from airflow import DAG
import datetime as dt

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': dt.timedelta(seconds=5),
        'start_date': dt.datetime(2020, 2, 2)
    }

with DAG(
    'main_dag',
    default_args = default_args,
    schedule_interval = None,
    max_active_runs = 1,
    catchup = False) as dag:
    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.utils.trigger_rule import TriggerRule
    from secrets_p5 import secrets
    import json