from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

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
    schedule_interval= '6 10 * * 3',
    # user_defined_filters={
    #     'localtz': local_tz,
    # },
    
)



start_dag = DummyOperator(task_id='start_dag', dag=dag)
