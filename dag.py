from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
import urllib3

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

local_tz = pendulum.timezone('America/Sao_Paulo')

#--------------------------------------------------------------------------------

dag = DAG(
    dag_id='test',
    description=f'Test',
    schedule_interval='0 23 * * *',
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
    

def print_first_file(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(task_ids='list_files_rf')
    pulled_value_first = pulled_value[0]
    pulled_value_second = pulled_value[1]
    print(pulled_value_first)
    print(pulled_value_second)


def download_files(**kwargs):
    ti = kwargs['ti']
    file_number = kwargs.get('file_number')
    pubsub_message = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]
    print(pubsub_message)

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




# def zipextract(bucketname, file_name_):
def zipextract(**kwargs):
    ti = kwargs['ti']
    file_number = kwargs.get('file_number')
    bucket_name = kwargs.get('bucket_name')
    file_name_ = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]

    new_bucket_name = bucket_name
    file_name = "download_files/" + file_name_
    file_name_final = file_name_[:-4]
    new_blob_speacial_character = 'unzip_files_treated/'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

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




# def mv_blob(bucket_name, file_name_):
def mv_blob(**kwargs):
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
    ti = kwargs['ti']
    file_number = kwargs.get('file_number')
    bucket_name = kwargs.get('bucket_name')
    file_name_ = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]
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


# def remove_special_character(bucket_name, file_name_):
def remove_special_character(**kwargs):
    ti = kwargs['ti']
    file_number = kwargs.get('file_number')
    bucket_name = kwargs.get('bucket_name')
    file_name_ = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]
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

# remove_special_character(bucket_name=bucket_name, file_name=file_name, file_name_final=file_name_final, new_blob_speacial_character=new_blob_speacial_character)


def list_files_rf():

    http = urllib3.PoolManager()

    url = 'http://200.152.38.155/CNPJ/'
    response = http.request('GET', url)

    soup = BeautifulSoup(response.data, 'html.parser')
    results = soup.find_all('a')

    i=0
    list_files=[]
    for file in results:
        file_url = file['href']
        if file_url[0] in ['?', '/', 'L']:
            pass
        else:
            i=i+1
            list_files.append(file_url)

    print(list_files)
    return list_files


list_files_rf = PythonOperator(
    task_id='list_files_rf',
    dag=dag,
    python_callable=list_files_rf,
    )





download_file0 = PythonOperator(task_id='download_file0',dag=dag,python_callable=download_files,op_kwargs={"file_number":'0'})
download_file1 = PythonOperator(task_id='download_file1',dag=dag,python_callable=download_files,op_kwargs={"file_number":'1'})
download_file2 = PythonOperator(task_id='download_file2',dag=dag,python_callable=download_files,op_kwargs={"file_number":'2'})
download_file3 = PythonOperator(task_id='download_file3',dag=dag,python_callable=download_files,op_kwargs={"file_number":'3'})
download_file4 = PythonOperator(task_id='download_file4',dag=dag,python_callable=download_files,op_kwargs={"file_number":'4'})
download_file5 = PythonOperator(task_id='download_file5',dag=dag,python_callable=download_files,op_kwargs={"file_number":'5'})
download_file6 = PythonOperator(task_id='download_file6',dag=dag,python_callable=download_files,op_kwargs={"file_number":'6'})
# download_file7 = PythonOperator(task_id='download_file7',dag=dag,python_callable=download_files,op_kwargs={"file_number":'7'})
# download_file8 = PythonOperator(task_id='download_file8',dag=dag,python_callable=download_files,op_kwargs={"file_number":'8'})
# download_file9 = PythonOperator(task_id='download_file9',dag=dag,python_callable=download_files,op_kwargs={"file_number":'9'})
# download_file10 = PythonOperator(task_id='download_file10',dag=dag,python_callable=download_files,op_kwargs={"file_number":'10'})
# download_file11 = PythonOperator(task_id='download_file11',dag=dag,python_callable=download_files,op_kwargs={"file_number":'11'})
# download_file12 = PythonOperator(task_id='download_file12',dag=dag,python_callable=download_files,op_kwargs={"file_number":'12'})
# download_file13 = PythonOperator(task_id='download_file13',dag=dag,python_callable=download_files,op_kwargs={"file_number":'13'})
# download_file14 = PythonOperator(task_id='download_file14',dag=dag,python_callable=download_files,op_kwargs={"file_number":'14'})
# download_file15 = PythonOperator(task_id='download_file15',dag=dag,python_callable=download_files,op_kwargs={"file_number":'15'})
# download_file16 = PythonOperator(task_id='download_file16',dag=dag,python_callable=download_files,op_kwargs={"file_number":'16'})
# download_file17 = PythonOperator(task_id='download_file17',dag=dag,python_callable=download_files,op_kwargs={"file_number":'17'})
# download_file18 = PythonOperator(task_id='download_file18',dag=dag,python_callable=download_files,op_kwargs={"file_number":'18'})
# download_file19 = PythonOperator(task_id='download_file19',dag=dag,python_callable=download_files,op_kwargs={"file_number":'19'})
# download_file20 = PythonOperator(task_id='download_file20',dag=dag,python_callable=download_files,op_kwargs={"file_number":'20'})
# download_file21 = PythonOperator(task_id='download_file21',dag=dag,python_callable=download_files,op_kwargs={"file_number":'21'})
# download_file22 = PythonOperator(task_id='download_file22',dag=dag,python_callable=download_files,op_kwargs={"file_number":'22'})
# download_file23 = PythonOperator(task_id='download_file23',dag=dag,python_callable=download_files,op_kwargs={"file_number":'23'})
# download_file24 = PythonOperator(task_id='download_file24',dag=dag,python_callable=download_files,op_kwargs={"file_number":'24'})
# download_file25 = PythonOperator(task_id='download_file25',dag=dag,python_callable=download_files,op_kwargs={"file_number":'25'})
# download_file26 = PythonOperator(task_id='download_file26',dag=dag,python_callable=download_files,op_kwargs={"file_number":'26'})
# download_file27 = PythonOperator(task_id='download_file27',dag=dag,python_callable=download_files,op_kwargs={"file_number":'27'})
# download_file28 = PythonOperator(task_id='download_file28',dag=dag,python_callable=download_files,op_kwargs={"file_number":'28'})
# download_file29 = PythonOperator(task_id='download_file29',dag=dag,python_callable=download_files,op_kwargs={"file_number":'29'})
# download_file30 = PythonOperator(task_id='download_file30',dag=dag,python_callable=download_files,op_kwargs={"file_number":'30'})
# download_file31 = PythonOperator(task_id='download_file31',dag=dag,python_callable=download_files,op_kwargs={"file_number":'31'})
# download_file32 = PythonOperator(task_id='download_file32',dag=dag,python_callable=download_files,op_kwargs={"file_number":'32'})
# download_file33 = PythonOperator(task_id='download_file33',dag=dag,python_callable=download_files,op_kwargs={"file_number":'33'})
# download_file34 = PythonOperator(task_id='download_file34',dag=dag,python_callable=download_files,op_kwargs={"file_number":'34'})
# download_file35 = PythonOperator(task_id='download_file35',dag=dag,python_callable=download_files,op_kwargs={"file_number":'35'})
# download_file36 = PythonOperator(task_id='download_file36',dag=dag,python_callable=download_files,op_kwargs={"file_number":'36'})
# download_file37 = PythonOperator(task_id='download_file37',dag=dag,python_callable=download_files,op_kwargs={"file_number":'37'})
# download_file38 = PythonOperator(task_id='download_file38',dag=dag,python_callable=download_files,op_kwargs={"file_number":'38'})
# download_file39 = PythonOperator(task_id='download_file39',dag=dag,python_callable=download_files,op_kwargs={"file_number":'39'})
# download_file40 = PythonOperator(task_id='download_file40',dag=dag,python_callable=download_files,op_kwargs={"file_number":'40'})




unzip_file0 = PythonOperator(
    task_id='unzip_file0',
    dag=dag,
    python_callable=zipextract,
    op_kwargs={"file_number":'0', "bucket_name": "cnpj_rf"},
    )


unzip_file1 = PythonOperator(
    task_id='unzip_file1',
    dag=dag,
    python_callable=zipextract,
    op_kwargs={"file_number":'1', "bucket_name": "cnpj_rf"},
    )


mv_file0 = PythonOperator(
    task_id='mv_file0',
    dag=dag,
    python_callable=mv_blob,
    op_kwargs={"bucket_name":'cnpj_rf', "file_number":'0'},
    )

mv_file1 = PythonOperator(
    task_id='mv_file1',
    dag=dag,
    python_callable=mv_blob,
    op_kwargs={"bucket_name":'cnpj_rf', "file_number":'1'},
    )

remove_special_char0 = PythonOperator(
    task_id='remove_special_char0',
    dag=dag,
    python_callable=remove_special_character,
    op_kwargs={"bucket_name":'cnpj_rf', "file_number":'0'},
    )

remove_special_char1 = PythonOperator(
    task_id='remove_special_char1',
    dag=dag,
    python_callable=remove_special_character,
    op_kwargs={"bucket_name":'cnpj_rf', "file_number":'1'},
    )




start_dag >> list_files_rf >> download_file0 >> unzip_file0 >> mv_file0 >> remove_special_char0
start_dag >> list_files_rf >> download_file1 >> unzip_file1 >> mv_file1 >> remove_special_char1
start_dag >> list_files_rf >> download_file2
start_dag >> list_files_rf >> download_file3 
start_dag >> list_files_rf >> download_file4 
start_dag >> list_files_rf >> download_file5 
start_dag >> list_files_rf >> download_file6 


