from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator
import psycopg2

from scripts.scripts import download_files
from scripts.scripts import zipextract
from scripts.scripts import mv_blob
from scripts.scripts import remove_special_character


# ***** IMPORTANT ******
# This makes our dag timezone aware
# Pass the 'local_tz' object to the datetime on the DAG.start_date
# Ex.: start_date=datetime(YEAR, MONTH, DAY, tzinfo=local_tz)
import pendulum

#-------------------------------------------------------------------------------
# Settings 
default_args = {
 'owner': 'Victor Paulillo',
 'retries': 1,
 'retry_delay': timedelta(minutes=5),
 'email': ['victor.paulilllo@gmail.com'],
 'email_on_failure': True
}

local_tz = pendulum.timezone('America/Toronto')

#--------------------------------------------------------------------------------

dag = DAG(
    dag_id='test',
    description=f'Test',
    schedule_interval='0 10 * * *',
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
)

# Start the DAG
start_dag = DummyOperator(task_id='start_dag', dag=dag)

# # BashOperator to list all files on the Google Cloud Storage
# gcs_files = BashOperator(
#     task_id="gcs_files",
#     bash_command=f"gsutil ls gs://cnpj_rf/download_files |  tr '\n' '||'",
#     xcom_push=True,
#     dag=dag
# )


#PythonOperator that runs the truncate funtion
download_file0 = PythonOperator(
    task_id='download_file0',
    dag=dag,
    python_callable=download_files,
    op_kwargs={"pubsub_message":'F.K03200$Z.D11009.CNAECSV.zip'},
    )

#PythonOperator that runs the truncate funtion
download_file1 = PythonOperator(
    task_id='download_file1',
    dag=dag,
    python_callable=download_files,
    op_kwargs={"pubsub_message":'F.K03200$Z.D11009.MUNICCSV.zip'},
    )

#PythonOperator that runs the truncate funtion
# download_file2 = PythonOperator(
#     task_id='download_file2',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y2.D11009.ESTABELE.zip'},
# )

# #PythonOperator that runs the truncate funtion
# download_file3 = PythonOperator(
#     task_id='download_file3',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y3.D11009.ESTABELE.zip'},
#     )

# #PythonOperator that runs the truncate funtion
# download_file4 = PythonOperator(
#     task_id='download_file4',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y4.D10911.ESTABELE.zip'},
# )
    
# #PythonOperator that runs the truncate funtion
# download_file5 = PythonOperator(
#     task_id='download_file5',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y5.D10911.ESTABELE.zip'},
#     )

# #PythonOperator that runs the truncate funtion
# download_file6 = PythonOperator(
#     task_id='download_file6',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y6.D10911.ESTABELE.zip'},
#     )

# #PythonOperator that runs the truncate funtion
# download_file7 = PythonOperator(
#     task_id='download_file7',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y7.D10911.ESTABELE.zip'},
#     )

# #PythonOperator that runs the truncate funtion
# download_file8 = PythonOperator(
#     task_id='download_file8',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y8.D10911.ESTABELE.zip'},
#     )

# #PythonOperator that runs the truncate funtion
# download_file9 = PythonOperator(
#     task_id='download_file9',
#     dag=dag,
#     python_callable=download_files,
#     op_kwargs={"pubsub_message":'K3241.K03200Y9.D10911.ESTABELE.zip'},
#     )



start_dag >> [download_file0, download_file1]
# , download_file2, download_file3]
# download_file4, download_file5, download_file6, download_file7, download_file8, download_file9]


# #Function that connects with Postgres and truncate the stage table
# def truncate_stage_table():
#     conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
#     cur = conn.cursor()
#     cur.execute("truncate prod_external.xignite_daily_adjusted_price_augmented_stage")
#     conn.commit()
#     cur.close()
#     conn.close()
#     return None

# #PythonOperator that runs the truncate funtion
# truncate_stage_table = PythonOperator(
#     task_id='truncate_stage_table',
#     dag=dag,
#     python_callable=truncate_stage_table,
#     )

# # Function to join the files found on Google Cloud Storage and add it on one string bash command
# def gcs_files_create_bash_command(**context):
#     gcs_files=context['templates_dict']['gcs_path_filename']
#     list_files=gcs_files.split("|")[1:-1]
#     database='composer'
#     table='prod_external.xignite_daily_adjusted_price_augmented_stage'
#     gcloud_import_command = ''
#     for file in list_files:
#         import_file = 'gcloud sql import csv composer-postgres {} --database={} --table={} ; '.format(file, database, table)
#         gcloud_import_command = gcloud_import_command + import_file
#     return gcloud_import_command[:-2]

# # Run the function that join on one string the files found on Google Cloud Storage
# gcs_files_create_bash_command = PythonOperator(
#     task_id='gcs_files_create_bash_command',
#     dag=dag,
#     python_callable=gcs_files_create_bash_command,
#     provide_context=True,  
#     templates_dict={'gcs_path_filename': "{{ ti.xcom_pull(task_ids='gcs_files') }}" },
#     xcom_push=True,
#     )

# # Get the result from the join files function
# xcom_get_import_command = '{{ ti.xcom_pull(task_ids="gcs_files_create_bash_command")}}'

# #BashOperator to import the files from the GCS to Postgres stage table. It runs the bash command string with the multiple files
# import_files_stage = BashOperator(
#     task_id="import_files_stage",
#     bash_command=xcom_get_import_command,
#     retries=2,
#     retry_delay=timedelta(minutes=2),
#     dag=dag
# )

# #Function to insert the data from stage table to production table 
# def insert_stage_data_into_prod():
#     conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
#     cur = conn.cursor()
#     cur.execute("""INSERT INTO prod_external.xignite_daily_adjusted_price_augmented
#                     SELECT s.*
#                     FROM prod_external.xignite_daily_adjusted_price_augmented_stage as s
#                     left join prod_external.xignite_daily_adjusted_price_augmented as p
#                     on s.ID = p.ID
#                     where p.ID is null
#                     ON CONFLICT (ID) DO NOTHING
#                     """)
#     conn.commit()
#     cur.close()
#     conn.close()
#     return None

# #PythonOperator to run the function that insert the data from stage table to production table 
# insert_stage_data_into_prod = PythonOperator(
#     task_id='insert_stage_data_into_prod',
#     dag=dag,
#     python_callable=insert_stage_data_into_prod,
#     )

# #Workflow Dependencies
# start_dag >> snowflake_copy_into_gcs >> gcs_files >> truncate_stage_table >> gcs_files_create_bash_command >> import_files_stage >> insert_stage_data_into_prod



