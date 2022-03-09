from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import pendulum



#-------------------------------------------------------------------------------
# Settings 
default_args = {
 'owner': 'Victor Paulillo',
 'retries': 5,
 'retry_delay': timedelta(minutes=5),
 'email': ['victor.paulilllo@gmail.com'],
 'email_on_failure': True
}

local_tz = pendulum.timezone('America/Sao_Paulo')

#--------------------------------------------------------------------------------

dag = DAG(
    dag_id='dag_test',
    description=f'DAG para testar o scheduler',
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
    schedule_interval= '0 23 * * 3',
)



start_dag = DummyOperator(task_id='start_dag', dag=dag)


import os
def db_pass():
    database_url = os.environ.get('DB_PASS')
    database_url2 = os.environ.get('AIRFLOW_CONN_PASS')
    database_url3 = os.environ.get('AIRFLOW_CONN_HOST')
    print(database_url)
    print(database_url2)
    print(database_url3)
    return database_url, database_url2, database_url3

bigquery_to_storage = PythonOperator(
    task_id='db_pass',
    dag=dag,
    python_callable=db_pass,
    provide_context=True
    )


    