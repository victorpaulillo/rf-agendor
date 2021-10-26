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


import requests
from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import bigquery
import re
from zipfile import ZipFile, is_zipfile
import io

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

def remove_spec_char(**kwargs):
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
    null_files = ['', '', '', '', '']
    list_files.extend(null_files)
    print(list_files)
    return list_files


# Start the DAG
start_dag = DummyOperator(task_id='start_dag', dag=dag)
remove_special_char_phase = DummyOperator(task_id='remove_special_char_phase', dag=dag)

list_files_rf = PythonOperator(task_id='list_files_rf',dag=dag,python_callable=list_files_rf)


download_file0 = PythonOperator(task_id='download_file0',dag=dag,python_callable=download_files,op_kwargs={"file_number":'0'})
download_file1 = PythonOperator(task_id='download_file1',dag=dag,python_callable=download_files,op_kwargs={"file_number":'1'})
download_file2 = PythonOperator(task_id='download_file2',dag=dag,python_callable=download_files,op_kwargs={"file_number":'2'})
download_file3 = PythonOperator(task_id='download_file3',dag=dag,python_callable=download_files,op_kwargs={"file_number":'3'})
download_file4 = PythonOperator(task_id='download_file4',dag=dag,python_callable=download_files,op_kwargs={"file_number":'4'})
download_file5 = PythonOperator(task_id='download_file5',dag=dag,python_callable=download_files,op_kwargs={"file_number":'5'})
download_file6 = PythonOperator(task_id='download_file6',dag=dag,python_callable=download_files,op_kwargs={"file_number":'6'})
download_file7 = PythonOperator(task_id='download_file7',dag=dag,python_callable=download_files,op_kwargs={"file_number":'7'})
download_file8 = PythonOperator(task_id='download_file8',dag=dag,python_callable=download_files,op_kwargs={"file_number":'8'})
download_file9 = PythonOperator(task_id='download_file9',dag=dag,python_callable=download_files,op_kwargs={"file_number":'9'})
download_file10 = PythonOperator(task_id='download_file10',dag=dag,python_callable=download_files,op_kwargs={"file_number":'10'})
download_file11 = PythonOperator(task_id='download_file11',dag=dag,python_callable=download_files,op_kwargs={"file_number":'11'})
download_file12 = PythonOperator(task_id='download_file12',dag=dag,python_callable=download_files,op_kwargs={"file_number":'12'})
download_file13 = PythonOperator(task_id='download_file13',dag=dag,python_callable=download_files,op_kwargs={"file_number":'13'})
download_file14 = PythonOperator(task_id='download_file14',dag=dag,python_callable=download_files,op_kwargs={"file_number":'14'})
download_file15 = PythonOperator(task_id='download_file15',dag=dag,python_callable=download_files,op_kwargs={"file_number":'15'})
download_file16 = PythonOperator(task_id='download_file16',dag=dag,python_callable=download_files,op_kwargs={"file_number":'16'})
download_file17 = PythonOperator(task_id='download_file17',dag=dag,python_callable=download_files,op_kwargs={"file_number":'17'})
download_file18 = PythonOperator(task_id='download_file18',dag=dag,python_callable=download_files,op_kwargs={"file_number":'18'})
download_file19 = PythonOperator(task_id='download_file19',dag=dag,python_callable=download_files,op_kwargs={"file_number":'19'})
download_file20 = PythonOperator(task_id='download_file20',dag=dag,python_callable=download_files,op_kwargs={"file_number":'20'})
download_file21 = PythonOperator(task_id='download_file21',dag=dag,python_callable=download_files,op_kwargs={"file_number":'21'})
download_file22 = PythonOperator(task_id='download_file22',dag=dag,python_callable=download_files,op_kwargs={"file_number":'22'})
download_file23 = PythonOperator(task_id='download_file23',dag=dag,python_callable=download_files,op_kwargs={"file_number":'23'})
download_file24 = PythonOperator(task_id='download_file24',dag=dag,python_callable=download_files,op_kwargs={"file_number":'24'})
download_file25 = PythonOperator(task_id='download_file25',dag=dag,python_callable=download_files,op_kwargs={"file_number":'25'})
download_file26 = PythonOperator(task_id='download_file26',dag=dag,python_callable=download_files,op_kwargs={"file_number":'26'})
download_file27 = PythonOperator(task_id='download_file27',dag=dag,python_callable=download_files,op_kwargs={"file_number":'27'})
download_file28 = PythonOperator(task_id='download_file28',dag=dag,python_callable=download_files,op_kwargs={"file_number":'28'})
download_file29 = PythonOperator(task_id='download_file29',dag=dag,python_callable=download_files,op_kwargs={"file_number":'29'})
download_file30 = PythonOperator(task_id='download_file30',dag=dag,python_callable=download_files,op_kwargs={"file_number":'30'})
download_file31 = PythonOperator(task_id='download_file31',dag=dag,python_callable=download_files,op_kwargs={"file_number":'31'})
download_file32 = PythonOperator(task_id='download_file32',dag=dag,python_callable=download_files,op_kwargs={"file_number":'32'})
download_file33 = PythonOperator(task_id='download_file33',dag=dag,python_callable=download_files,op_kwargs={"file_number":'33'})
download_file34 = PythonOperator(task_id='download_file34',dag=dag,python_callable=download_files,op_kwargs={"file_number":'34'})
download_file35 = PythonOperator(task_id='download_file35',dag=dag,python_callable=download_files,op_kwargs={"file_number":'35'})
download_file36 = PythonOperator(task_id='download_file36',dag=dag,python_callable=download_files,op_kwargs={"file_number":'36'})
download_file37 = PythonOperator(task_id='download_file37',dag=dag,python_callable=download_files,op_kwargs={"file_number":'37'})
download_file38 = PythonOperator(task_id='download_file38',dag=dag,python_callable=download_files,op_kwargs={"file_number":'38'})
download_file39 = PythonOperator(task_id='download_file39',dag=dag,python_callable=download_files,op_kwargs={"file_number":'39'})
download_file40 = PythonOperator(task_id='download_file40',dag=dag,python_callable=download_files,op_kwargs={"file_number":'40'})


unzip_file0 = PythonOperator(task_id='unzip_file0',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'0', "bucket_name": "cnpj_rf"})
unzip_file1 = PythonOperator(task_id='unzip_file1',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'1', "bucket_name": "cnpj_rf"})
unzip_file2 = PythonOperator(task_id='unzip_file2',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'2', "bucket_name": "cnpj_rf"})
unzip_file3 = PythonOperator(task_id='unzip_file3',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'3', "bucket_name": "cnpj_rf"})
unzip_file4 = PythonOperator(task_id='unzip_file4',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'4', "bucket_name": "cnpj_rf"})
unzip_file5 = PythonOperator(task_id='unzip_file5',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'5', "bucket_name": "cnpj_rf"})
unzip_file6 = PythonOperator(task_id='unzip_file6',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'6', "bucket_name": "cnpj_rf"})
unzip_file7 = PythonOperator(task_id='unzip_file7',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'7', "bucket_name": "cnpj_rf"})
unzip_file8 = PythonOperator(task_id='unzip_file8',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'8', "bucket_name": "cnpj_rf"})
unzip_file9 = PythonOperator(task_id='unzip_file9',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'9', "bucket_name": "cnpj_rf"})
unzip_file10 = PythonOperator(task_id='unzip_file10',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'10', "bucket_name": "cnpj_rf"})
unzip_file11 = PythonOperator(task_id='unzip_file11',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'11', "bucket_name": "cnpj_rf"})
unzip_file12 = PythonOperator(task_id='unzip_file12',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'12', "bucket_name": "cnpj_rf"})
unzip_file13 = PythonOperator(task_id='unzip_file13',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'13', "bucket_name": "cnpj_rf"})
unzip_file14 = PythonOperator(task_id='unzip_file14',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'14', "bucket_name": "cnpj_rf"})
unzip_file15 = PythonOperator(task_id='unzip_file15',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'15', "bucket_name": "cnpj_rf"})
unzip_file16 = PythonOperator(task_id='unzip_file16',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'16', "bucket_name": "cnpj_rf"})
unzip_file17 = PythonOperator(task_id='unzip_file17',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'17', "bucket_name": "cnpj_rf"})
unzip_file18 = PythonOperator(task_id='unzip_file18',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'18', "bucket_name": "cnpj_rf"})
unzip_file19 = PythonOperator(task_id='unzip_file19',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'19', "bucket_name": "cnpj_rf"})
unzip_file20 = PythonOperator(task_id='unzip_file20',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'20', "bucket_name": "cnpj_rf"})
unzip_file21 = PythonOperator(task_id='unzip_file21',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'21', "bucket_name": "cnpj_rf"})
unzip_file22 = PythonOperator(task_id='unzip_file22',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'22', "bucket_name": "cnpj_rf"})
unzip_file23 = PythonOperator(task_id='unzip_file23',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'23', "bucket_name": "cnpj_rf"})
unzip_file24 = PythonOperator(task_id='unzip_file24',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'24', "bucket_name": "cnpj_rf"})
unzip_file25 = PythonOperator(task_id='unzip_file25',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'25', "bucket_name": "cnpj_rf"})
unzip_file26 = PythonOperator(task_id='unzip_file26',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'26', "bucket_name": "cnpj_rf"})
unzip_file27 = PythonOperator(task_id='unzip_file27',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'27', "bucket_name": "cnpj_rf"})
unzip_file28 = PythonOperator(task_id='unzip_file28',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'28', "bucket_name": "cnpj_rf"})
unzip_file29 = PythonOperator(task_id='unzip_file29',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'29', "bucket_name": "cnpj_rf"})
unzip_file30 = PythonOperator(task_id='unzip_file30',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'30', "bucket_name": "cnpj_rf"})
unzip_file31 = PythonOperator(task_id='unzip_file31',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'31', "bucket_name": "cnpj_rf"})
unzip_file32 = PythonOperator(task_id='unzip_file32',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'32', "bucket_name": "cnpj_rf"})
unzip_file33 = PythonOperator(task_id='unzip_file33',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'33', "bucket_name": "cnpj_rf"})
unzip_file34 = PythonOperator(task_id='unzip_file34',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'34', "bucket_name": "cnpj_rf"})
unzip_file35 = PythonOperator(task_id='unzip_file35',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'35', "bucket_name": "cnpj_rf"})
unzip_file36 = PythonOperator(task_id='unzip_file36',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'36', "bucket_name": "cnpj_rf"})
unzip_file37 = PythonOperator(task_id='unzip_file37',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'37', "bucket_name": "cnpj_rf"})
unzip_file38 = PythonOperator(task_id='unzip_file38',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'38', "bucket_name": "cnpj_rf"})
unzip_file39 = PythonOperator(task_id='unzip_file39',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'39', "bucket_name": "cnpj_rf"})
unzip_file40 = PythonOperator(task_id='unzip_file40',dag=dag,python_callable=zipextract,op_kwargs={"file_number":'40', "bucket_name": "cnpj_rf"})


mv_file0 = PythonOperator(task_id='mv_file0',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'0'})
mv_file1 = PythonOperator(task_id='mv_file1',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'1'})
mv_file2 = PythonOperator(task_id='mv_file2',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'2'})
mv_file3 = PythonOperator(task_id='mv_file3',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'3'})
mv_file4 = PythonOperator(task_id='mv_file4',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'4'})
mv_file5 = PythonOperator(task_id='mv_file5',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'5'})
mv_file6 = PythonOperator(task_id='mv_file6',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'6'})
mv_file7 = PythonOperator(task_id='mv_file7',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'7'})
mv_file8 = PythonOperator(task_id='mv_file8',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'8'})
mv_file9 = PythonOperator(task_id='mv_file9',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'9'})
mv_file10 = PythonOperator(task_id='mv_file10',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'10'})
mv_file11 = PythonOperator(task_id='mv_file11',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'11'})
mv_file12 = PythonOperator(task_id='mv_file12',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'12'})
mv_file13 = PythonOperator(task_id='mv_file13',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'13'})
mv_file14 = PythonOperator(task_id='mv_file14',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'14'})
mv_file15 = PythonOperator(task_id='mv_file15',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'15'})
mv_file16 = PythonOperator(task_id='mv_file16',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'16'})
mv_file17 = PythonOperator(task_id='mv_file17',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'17'})
mv_file18 = PythonOperator(task_id='mv_file18',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'18'})
mv_file19 = PythonOperator(task_id='mv_file19',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'19'})
mv_file20 = PythonOperator(task_id='mv_file20',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'20'})
mv_file21 = PythonOperator(task_id='mv_file21',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'21'})
mv_file22 = PythonOperator(task_id='mv_file22',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'22'})
mv_file23 = PythonOperator(task_id='mv_file23',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'23'})
mv_file24 = PythonOperator(task_id='mv_file24',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'24'})
mv_file25 = PythonOperator(task_id='mv_file25',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'25'})
mv_file26 = PythonOperator(task_id='mv_file26',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'26'})
mv_file27 = PythonOperator(task_id='mv_file27',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'27'})
mv_file28 = PythonOperator(task_id='mv_file28',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'28'})
mv_file29 = PythonOperator(task_id='mv_file29',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'29'})
mv_file30 = PythonOperator(task_id='mv_file30',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'30'})
mv_file31 = PythonOperator(task_id='mv_file31',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'31'})
mv_file32 = PythonOperator(task_id='mv_file32',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'32'})
mv_file33 = PythonOperator(task_id='mv_file33',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'33'})
mv_file34 = PythonOperator(task_id='mv_file34',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'34'})
mv_file35 = PythonOperator(task_id='mv_file35',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'35'})
mv_file36 = PythonOperator(task_id='mv_file36',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'36'})
mv_file37 = PythonOperator(task_id='mv_file37',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'37'})
mv_file38 = PythonOperator(task_id='mv_file38',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'38'})
mv_file39 = PythonOperator(task_id='mv_file39',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'39'})
mv_file40 = PythonOperator(task_id='mv_file40',dag=dag,python_callable=mv_blob,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'40'})


remove_spec_char0 = PythonOperator(task_id='remove_spec_char0',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'0'})
remove_spec_char1 = PythonOperator(task_id='remove_spec_char1',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'1'})
remove_spec_char2 = PythonOperator(task_id='remove_spec_char2',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'2'})
remove_spec_char3 = PythonOperator(task_id='remove_spec_char3',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'3'})
remove_spec_char4 = PythonOperator(task_id='remove_spec_char4',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'4'})
remove_spec_char5 = PythonOperator(task_id='remove_spec_char5',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'5'})
remove_spec_char6 = PythonOperator(task_id='remove_spec_char6',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'6'})
remove_spec_char7 = PythonOperator(task_id='remove_spec_char7',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'7'})
remove_spec_char8 = PythonOperator(task_id='remove_spec_char8',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'8'})
remove_spec_char9 = PythonOperator(task_id='remove_spec_char9',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'9'})
remove_spec_char10 = PythonOperator(task_id='remove_spec_char10',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'10'})
remove_spec_char11 = PythonOperator(task_id='remove_spec_char11',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'11'})
remove_spec_char12 = PythonOperator(task_id='remove_spec_char12',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'12'})
remove_spec_char13 = PythonOperator(task_id='remove_spec_char13',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'13'})
remove_spec_char14 = PythonOperator(task_id='remove_spec_char14',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'14'})
remove_spec_char15 = PythonOperator(task_id='remove_spec_char15',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'15'})
remove_spec_char16 = PythonOperator(task_id='remove_spec_char16',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'16'})
remove_spec_char17 = PythonOperator(task_id='remove_spec_char17',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'17'})
remove_spec_char18 = PythonOperator(task_id='remove_spec_char18',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'18'})
remove_spec_char19 = PythonOperator(task_id='remove_spec_char19',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'19'})
remove_spec_char20 = PythonOperator(task_id='remove_spec_char20',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'20'})
remove_spec_char21 = PythonOperator(task_id='remove_spec_char21',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'21'})
remove_spec_char22 = PythonOperator(task_id='remove_spec_char22',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'22'})
remove_spec_char23 = PythonOperator(task_id='remove_spec_char23',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'23'})
remove_spec_char24 = PythonOperator(task_id='remove_spec_char24',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'24'})
remove_spec_char25 = PythonOperator(task_id='remove_spec_char25',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'25'})
remove_spec_char26 = PythonOperator(task_id='remove_spec_char26',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'26'})
remove_spec_char27 = PythonOperator(task_id='remove_spec_char27',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'27'})
remove_spec_char28 = PythonOperator(task_id='remove_spec_char28',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'28'})
remove_spec_char29 = PythonOperator(task_id='remove_spec_char29',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'29'})
remove_spec_char30 = PythonOperator(task_id='remove_spec_char30',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'30'})
remove_spec_char31 = PythonOperator(task_id='remove_spec_char31',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'31'})
remove_spec_char32 = PythonOperator(task_id='remove_spec_char32',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'32'})
remove_spec_char33 = PythonOperator(task_id='remove_spec_char33',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'33'})
remove_spec_char34 = PythonOperator(task_id='remove_spec_char34',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'34'})
remove_spec_char35 = PythonOperator(task_id='remove_spec_char35',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'35'})
remove_spec_char36 = PythonOperator(task_id='remove_spec_char36',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'36'})
remove_spec_char37 = PythonOperator(task_id='remove_spec_char37',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'37'})
remove_spec_char38 = PythonOperator(task_id='remove_spec_char38',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'38'})
remove_spec_char39 = PythonOperator(task_id='remove_spec_char39',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'39'})
remove_spec_char40 = PythonOperator(task_id='remove_spec_char40',dag=dag,python_callable=remove_spec_char,op_kwargs={"bucket_name":'cnpj_rf', "file_number":'40'})


start_dag >> list_files_rf >> download_file0 >> unzip_file0 >> mv_file0 >> remove_special_char_phase
start_dag >> list_files_rf >> download_file1 >> unzip_file1 >> mv_file1 >> download_file5 >> unzip_file5 >> mv_file5 >> download_file9 >> unzip_file9 >> mv_file9    >> download_file13 >> unzip_file13 >> mv_file13 >> download_file17 >> unzip_file17 >> mv_file17 >> download_file21 >> unzip_file21 >> mv_file21 >> download_file25 >> unzip_file25 >> mv_file25 >> download_file29 >> unzip_file29 >> mv_file29 >> download_file33 >> unzip_file33 >> mv_file33 >> download_file37 >> unzip_file37 >> mv_file37 >> remove_special_char_phase
start_dag >> list_files_rf >> download_file2 >> unzip_file2 >> mv_file2 >> download_file6 >> unzip_file6 >> mv_file6 >> download_file10 >> unzip_file10 >> mv_file10 >> download_file14 >> unzip_file14 >> mv_file14 >> download_file18 >> unzip_file18 >> mv_file18 >> download_file22 >> unzip_file22 >> mv_file22 >> download_file26 >> unzip_file26 >> mv_file26 >> download_file30 >> unzip_file30 >> mv_file30 >> download_file34 >> unzip_file34 >> mv_file34 >> download_file38 >> unzip_file38 >> mv_file38 >> remove_special_char_phase
start_dag >> list_files_rf >> download_file3 >> unzip_file3 >> mv_file3 >> download_file7 >> unzip_file7 >> mv_file7 >> download_file11 >> unzip_file11 >> mv_file11 >> download_file15 >> unzip_file15 >> mv_file15 >> download_file19 >> unzip_file19 >> mv_file19 >> download_file23 >> unzip_file23 >> mv_file23 >> download_file27 >> unzip_file27 >> mv_file27 >> download_file31 >> unzip_file31 >> mv_file31 >> download_file35 >> unzip_file35 >> mv_file35 >> download_file39 >> unzip_file39 >> mv_file39 >> remove_special_char_phase
start_dag >> list_files_rf >> download_file4 >> unzip_file4 >> mv_file4 >> download_file8 >> unzip_file8 >> mv_file8 >> download_file12 >> unzip_file12 >> mv_file12 >> download_file16 >> unzip_file16 >> mv_file16 >> download_file20 >> unzip_file20 >> mv_file20 >> download_file24 >> unzip_file24 >> mv_file24 >> download_file28 >> unzip_file28 >> mv_file28 >> download_file32 >> unzip_file32 >> mv_file32 >> download_file36 >> unzip_file36 >> mv_file36 >> download_file40 >> unzip_file40 >> mv_file40 >> remove_special_char_phase


remove_special_char_phase >> remove_spec_char0 >> remove_spec_char1 >> remove_spec_char2 >> remove_spec_char3 >> remove_spec_char4 >> remove_spec_char5 >> remove_spec_char6 >> remove_spec_char7 >> remove_spec_char8 >> remove_spec_char9 >> remove_spec_char10 >> remove_spec_char11 >> remove_spec_char12 >> remove_spec_char13 >> remove_spec_char14 >> remove_spec_char15 >> remove_spec_char16 >> remove_spec_char17 >> remove_spec_char18 >> remove_spec_char19 >> remove_spec_char20 >> remove_spec_char21 >> remove_spec_char22 >> remove_spec_char23 >> remove_spec_char24 >> remove_spec_char25 >> remove_spec_char26 >> remove_spec_char27 >> remove_spec_char28 >> remove_spec_char29 >> remove_spec_char30 >> remove_spec_char31 >> remove_spec_char32 >> remove_spec_char33 >> remove_spec_char34 >> remove_spec_char35 >> remove_spec_char36 >> remove_spec_char37 >> remove_spec_char38 >> remove_spec_char39 >> remove_spec_char40 

