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
    from secrets_p5 import secrets
    import json
    import pandas as pd
    import requests
    from sqlalchemy import create_engine, text

    def save_params(**kwarg):
        current_datetime = dt.datetime.now()
        seven_days_ago = current_datetime - dt.timedelta(days=7)
        current_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        seven_days_ago = seven_days_ago.strftime("%Y-%m-%d %H:%M:%S")
        kwarg['ti'].xcom_push(key='current_datetime', value = f'{current_datetime}')
        kwarg['ti'].xcom_push(key='seven_days_ago', value = f'{seven_days_ago}')
        for key, value in kwarg.items():
            kwarg['ti'].xcom_push(key=f'{key}', value = f'{value}')

    def execute_sql(sql,**kwarg):
        pg_user = kwarg['ti'].xcom_pull(key='pg_user')
        pg_password = kwarg['ti'].xcom_pull(key='pg_password')
        pg_host = kwarg['ti'].xcom_pull(key='pg_host')
        pg_port = kwarg['ti'].xcom_pull(key='pg_port')
        pg_base_name = kwarg['ti'].xcom_pull(key='pg_base_name')
        sql = sql
        engine = create_engine(
            f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_base_name}', 
            future=True)
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            conn.commit()
        engine.dispose()

    def get_data_from_api(kwarg,**context):
        api_key = context['ti'].xcom_pull(key='api_key')
        api_endpoint = context['ti'].xcom_pull(key='api_endpoint')
        nickname = context['ti'].xcom_pull(key='nickname')
        cohort = context['ti'].xcom_pull(key='cohort')
        pg_user = context['ti'].xcom_pull(key='pg_user')
        pg_password = context['ti'].xcom_pull(key='pg_password')
        pg_host = context['ti'].xcom_pull(key='pg_host')
        pg_port = context['ti'].xcom_pull(key='pg_port')
        pg_base_name = context['ti'].xcom_pull(key='pg_base_name')
        api_method = kwarg['api_method']
        schema_name = kwarg['schema_name']
        table_name = kwarg['table_name']
        params = kwarg['params']
        print(params)
        offset = 0
        headers = {'X-Nickname': nickname, 
                   'X-Cohort': cohort, 
                   'X-API-KEY': api_key
                   }
        sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
        execute_sql(sql,**context)
        start_time = dt.datetime.now()
        while (dt.datetime.now() - start_time).total_seconds() < 30:
            res = requests.get('https://'+api_endpoint + api_method,
                            headers = headers, 
                            params = params
                            )
            res_json = json.loads(res.content)
            print(res_json)
            offset = offset + len(res_json)
            params['offset'] = offset
            if len(res_json) == 0:
                break
            df = pd.DataFrame(res_json)
            engine = create_engine(
                f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_base_name}', 
                future=True)
            df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
            engine.dispose()

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
        'dds_schema_name': secrets['dds_schema_name'],
        'dm_schema_name': secrets['dm_schema_name'],
        },
        dag = dag
        )
    
    get_data_from_api_couriers = PythonOperator(
        task_id = 'get_data_from_api_couriers',
        python_callable = get_data_from_api,
        provide_context=True,
        op_kwargs = {
            'kwarg':{
                'api_method': '/couriers',
                'schema_name': secrets['stage_schema_name'],
                'table_name': 'api_couriers',
                'params': {'sort_field':'_id', 
                        'sort_direction':'asc',
                        'limit': 50}
            }
        },
        dag = dag
        )

    get_data_from_api_deliveries = PythonOperator(
        task_id = 'get_data_from_api_deliveries',
        python_callable = get_data_from_api,
        provide_context=True,
        op_kwargs = {
            'kwarg':{
                'api_method': '/deliveries',
                'schema_name': secrets['stage_schema_name'],
                'table_name': 'api_deliveries',
                'params': {'sort_field':'date', 
                        'sort_direction':'asc',
                        'limit': 50,
                        'from': '{{ task_instance.xcom_pull(task_ids="save_params_t", key="seven_days_ago") }}',
                        'to': '{{ task_instance.xcom_pull(task_ids="save_params_t", key="current_datetime") }}'}
            }
        },
        dag = dag
        )
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> save_params_t >> [get_data_from_api_couriers, get_data_from_api_deliveries] >> end