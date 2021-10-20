
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





bucket_name = "cnpj_rf"
new_bucket_name = bucket_name
new_blob_speacial_character = 'unzip_files_treated/'
file_name_gs = 'F.K03200$Z.D11009.MUNICCSV.zip'
file_name = "download_files/" + file_name_gs
file_name_final = file_name_gs[:-4]






def zipextract(bucketname, file_name):

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


blob_name = "download_files/" + file_name_gs + "/" + file_name_final
new_blob_name = "unzip_files/" + file_name_final

def mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
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


def remove_special_character(bucket_name, file_name, file_name_final, new_blob_speacial_character):

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    blob = bucket.get_blob(file_name)
    file = blob.download_as_string()

    file_decoded = file.decode("ISO-8859-1")
    file_upload = re.sub(r"[^a-zA-Z0-9,-@+_ \"\n]", '', str(file_decoded))
    
    bucket.blob(new_blob_speacial_character + file_name_final).upload_from_string(file_upload, 'text/csv')
    
    print(f'File moved from {new_blob_name} to {new_blob_speacial_character}')

# remove_special_character(bucket_name=bucket_name, file_name=file_name, file_name_final=file_name_final, new_blob_speacial_character=new_blob_speacial_character)

