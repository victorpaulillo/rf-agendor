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


def remove_spec_char(**kwargs):
    ti = kwargs['ti']
    file_number = kwargs.get('file_number')
    bucket_name = kwargs.get('bucket_name')
    file_name_ = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]

    if file_name_ == '':
        None

    else:
        if 'ESTABELE' in file_name_:

            file_name_final = file_name_[:-4]
            blob_name = "unzip_files/" + file_name_final
            new_blob_special_character = "unzip_files_treated/" + file_name_final

            client = storage.Client()
            bucket = client.get_bucket(bucket_name)

            blob = bucket.get_blob(blob_name)
            file = blob.download_as_string()

            file_decoded = file.decode("ISO-8859-1")
            file_upload = re.sub(r"[^a-zA-Z0-9,-@+_ \"\n]", '', str(file_decoded))
            
            bucket.blob(new_blob_special_character).upload_from_string(file_upload, 'text/csv')
            
            print(f'File moved from {blob_name} to {new_blob_special_character}')

        else:
            print(f'Did nothing, file is not ESTABELE, it is {file_name_}')





start_dag = DummyOperator(task_id='start_dag', dag=dag)




def values_function():
    values = [0,1,2,3,4,5]
    return values

def group_remove_speacial_char(number, **kwargs):
        #load the values if needed in the command you plan to execute
        file_number = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
        return PythonOperator(task_id='remove_spec_char_{}'.format(file_number),
            dag=dag,python_callable=remove_spec_char,
            op_kwargs={"bucket_name":'cnpj_rf', "file_number":file_number}
            )
        
        

                

push_func = PythonOperator(
        task_id='push_func',
        provide_context=True,
        python_callable=values_function,
        dag=dag)


for i in values_function():
        push_func >> group_remove_speacial_char(i) 

