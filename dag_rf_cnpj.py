from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


from google.cloud import bigquery
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
    dag_id='dag_rf_cnpj',
    description=f'DAG para fazer o ETL e load dos dados de CNPJ da Receita Federal para uso da API de Cadastro do Agendor',
    schedule_interval=None,
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
)



start_dag = DummyOperator(task_id='start_dag', dag=dag)




def values_function():
     return 5

def group_remove_speacial_char(number, **kwargs):
        #load the values if needed in the command you plan to execute
        dyn_value = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
        return BashOperator(
                task_id='JOB_NAME_{}'.format(number),
                bash_command='script.sh {} {}'.format(dyn_value, number),
                dag=dag)
                

push_func = PythonOperator(
        task_id='push_func',
        provide_context=True,
        python_callable=values_function,
        dag=dag)

complete = DummyOperator(
        task_id='All_jobs_completed',
        dag=dag)

for i in values_function():
        push_func >> group_remove_speacial_char(i) >> complete

