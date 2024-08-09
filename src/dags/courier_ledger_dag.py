from airflow import DAG
import datetime as dt
from datetime import datetime

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
    'courier_ledger_dag',
    default_args = default_args,
    schedule_interval = '0/15 * * * *',
    max_active_runs = 1,
    catchup = False) as dag:

    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.utils.trigger_rule import TriggerRule
    from secrets_p5 import secrets
    import json
    import pandas as pd
    import requests
    from sqlalchemy import create_engine, text

    def save_params(**kwarg):
        for key, value in kwarg.items():
            kwarg['ti'].xcom_push(key=f'{key}', value = f'{value}')

    def execute_sql(**kwarg):
        pg_user = kwarg['ti'].xcom_pull(key='pg_user')
        pg_password = kwarg['ti'].xcom_pull(key='pg_password')
        pg_host = kwarg['ti'].xcom_pull(key='pg_host')
        pg_port = kwarg['ti'].xcom_pull(key='pg_port')
        pg_base_name = kwarg['ti'].xcom_pull(key='pg_base_name')
        stage_schema_name_f = kwarg['ti'].xcom_pull(key='stage_schema_name')
        dm_schema_name_f = kwarg['ti'].xcom_pull(key='dm_schema_name')
        sql = kwarg['sql']
        sql = sql.format(stage_schema_name = stage_schema_name_f, dm_schema_name = dm_schema_name_f)
        engine = create_engine(
            f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_base_name}', 
            future=True)
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            conn.commit()
        engine.dispose()

    def get_data_from_api(**kwarg):
        api_key = kwarg['ti'].xcom_pull(key='api_key')
        api_endpoint = kwarg['ti'].xcom_pull(key='api_endpoint')
        nickname = kwarg['ti'].xcom_pull(key='nickname')
        cohort = kwarg['ti'].xcom_pull(key='cohort')
        api_method = kwarg['api_method']
        offset = 0
        headers = {'X-Nickname': nickname, 
                   'X-Cohort': cohort, 
                   'X-API-KEY': api_key
                   }
        start_time = dt.datetime.now()
        while (dt.datetime.now() - start_time).total_seconds() < 30:
            res = requests.get('https://'+api_endpoint + api_method,
                               headers = headers, 
                               params = {'sort_field':'_id', 
                                         'sort_direction':'asc',
                                         'offset': offset,
                                         'limit': 30})
            res_json = json.loads(res.content)
            if len(res_json) == 0:
                break
            if offset == 0:
            df_all = pd.DataFrame(res_json)
            if offset > 0:
                df_new = pd.DataFrame(res_json)
                df_all = pd.concat([df_all, df_new], ignore_index=True)
            offset = offset + len(res_json)
        return df_all

    save_params_t = PythonOperator(
        task_id = 'save_params_t',
        python_callable = save_params,
        provide_context=True,
        op_kwargs = {
        'nickname': secrets['nickname'],
        'cohort': secrets['cohort'],
        'api_key': secrets['api_key'],
        'api_endpoint': secrets['api_endpoint'],
        'pg_host': secrets['pg_host'],
        'pg_port': secrets['pg_port'],
        'pg_base_name': secrets['pg_base_name'],
        'pg_user': secrets['pg_user'],
        'pg_password': secrets['pg_password'],
        'stage_schema_name': secrets['stage_schema_name'],
        'dm_schema_name': secrets['dm_schema_name'],
        },
        dag = dag
        )
    
