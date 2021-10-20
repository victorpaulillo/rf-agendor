from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator
import psycopg2

# from scripts.scripts import download_files
# from scripts.scripts import zipextract
# from scripts.scripts import mv_blob
# from scripts.scripts import remove_special_character


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











import requests
from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import bigquery
import re
from zipfile import ZipFile, is_zipfile
import io
    
    
def download_files(pubsub_message):

    start_time = datetime.now()
    print(start_time)
    url = 'http://200.152.38.155/CNPJ/' + pubsub_message 
    with requests.get(url, stream=True) as myfile:
        down_time = datetime.now()

    #open(pubsub_message, 'wb').write(myfile.content)
        with open(pubsub_message, "wb") as outfile:
            for chunk in myfile.iter_content(chunk_size=None):  # Let the server decide.
                outfile.write(chunk)
    download_time = datetime.now()

    print('Down time: ', down_time - start_time)
    print('Download time: ', download_time - start_time)
    print('Finished downloading')

    bucket_name = "cnpj_rf"
    file_name = pubsub_message
    destination_bucket_name = "download_files/"
    destination_blob_name = destination_bucket_name + file_name
    source_file_name = pubsub_message
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    upload_time = datetime.now()

    print('Upload time: ', upload_time-start_time)
    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))

    #Insert download file record into etl_jobs table on bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
            insert `fiery-marking-325513.rf.etl_jobs` (file_name, download_timestamp)
            values('""" + pubsub_message + """', timestamp(DATETIME(CURRENT_TIMESTAMP(), "America/Sao_Paulo")))
    """
    query_job = client.query(query)  # Make an API request.

    print(f"Inserted record on bigquery etl_jobs table with the query: {query}")

    """Publishes messages to a Pub/Sub topic"""

    # # TODO(developer)
    # project_id = "fiery-marking-325513"
    # topic_id = "downloaded_files"

    # publisher = pubsub_v1.PublisherClient()
    # topic_path = publisher.topic_path(project_id, topic_id)

    # data = str(pubsub_message)
    # # Data must be a bytestring
    # data = data.encode("utf-8")
    # # Add two attributes, origin and username, to the message
    # future = publisher.publish(
    #     topic_path, data, origin="python-sample", username="gcp"
    # )
    # print(future.result())

    # print(f"Published messages with custom attributes to {topic_path}.")

    return "Hello {}!".format(pubsub_message)




def zipextract(bucketname, file_name_):
    new_bucket_name = bucketname
    file_name = "download_files/" + file_name_
    file_name_final = file_name_[:-4]
    new_blob_speacial_character = 'unzip_files_treated/'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)

    destination_blob_pathname = file_name

    blob = bucket.blob(file_name)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(file_name + "/" + contentfilename)
                blob.upload_from_string(contentfile)
    
    print(f'File unzipped from {file_name}')

# zipextract(bucket_name, file_name) # if the file is gs://mybucket/path/file.zip




def mv_blob(bucket_name, file_name_):
    """
    Function for moving files between directories or buckets. it will use GCP's copy 
    function then delete the blob from the old location.
    
    inputs
    -----
    bucket_name: name of bucket
    blob_name: str, name of file 
        ex. 'data/some_location/file_name'
    new_bucket_name: name of bucket (can be same as original if we're just moving around directories)
    new_blob_name: str, name of file in new directory in target bucket 
        ex. 'data/destination/file_name'
    """
    new_bucket_name = bucket_name
    file_name_final = file_name_[:-4]
    blob_name = "download_files/" + file_name_ + "/" + file_name_final
    new_blob_name = "unzip_files/" + file_name_final

    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    # copy to new destination
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()
    
    print(f'File moved from {source_blob} to {new_blob_name}')
    
# mv_blob(bucket_name=bucket_name, blob_name=blob_name, new_bucket_name=new_bucket_name, new_blob_name=new_blob_name)


def remove_special_character(bucket_name, file_name_):
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

# remove_special_character(bucket_name=bucket_name, file_name=file_name, file_name_final=file_name_final, new_blob_speacial_character=new_blob_speacial_character)



























download_file0 = PythonOperator(
    task_id='download_file0',
    dag=dag,
    python_callable=download_files,
    op_kwargs={"pubsub_message":'F.K03200$Z.D11009.MUNICCSV.zip'},
    )

download_file1 = PythonOperator(
    task_id='download_file1',
    dag=dag,
    python_callable=download_files,
    op_kwargs={"pubsub_message":'F.K03200$Z.D11009.CNAECSV.zip'},
    )

unzip_file1 = PythonOperator(
    task_id='unzip_file1',
    dag=dag,
    python_callable=zipextract,
    op_kwargs={"file_name_":'F.K03200$Z.D11009.MUNICCSV.zip', "bucketname": "cnpj_rf"},
    )


unzip_file2 = PythonOperator(
    task_id='unzip_file2',
    dag=dag,
    python_callable=zipextract,
    op_kwargs={"file_name_":'F.K03200$Z.D11009.CNAECSV.zip', "bucketname": "cnpj_rf"},
    )


mv_file1 = PythonOperator(
    task_id='mv_file1',
    dag=dag,
    python_callable=mv_blob,
    op_kwargs={"bucket_name":'cnpj_rf', "file_name_": 'F.K03200$Z.D11009.MUNICCSV.zip'},
    )

mv_file2 = PythonOperator(
    task_id='mv_file2',
    dag=dag,
    python_callable=mv_blob,
    op_kwargs={"bucket_name":'cnpj_rf', "file_name_": 'F.K03200$Z.D11009.CNAECSV.zip'},
    )

remove_special_char1 = PythonOperator(
    task_id='remove_special_char1',
    dag=dag,
    python_callable=remove_special_character,
    op_kwargs={"bucket_name":'cnpj_rf', "file_name_": 'F.K03200$Z.D11009.MUNICCSV.zip'},
    )

remove_special_char2 = PythonOperator(
    task_id='remove_special_char2',
    dag=dag,
    python_callable=remove_special_character,
    op_kwargs={"bucket_name":'cnpj_rf', "file_name_": 'F.K03200$Z.D11009.CNAECSV.zip'},
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



start_dag >> download_file0 >> unzip_file1 >> mv_file1 >> remove_special_char1
start_dag >> download_file1 >> unzip_file2 >> mv_file2 >> remove_special_char2


# >> [unzip_file1, unzip_file2] >> [mv_file1, mv_file2]



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



