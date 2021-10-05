import os
import base64
import requests
from datetime import datetime
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import bigquery

from concurrent import futures


from flask import Flask, request

app = Flask(__name__)

# [START eventarc_pubsub_handler]
@app.route('/', methods=['POST'])
#def download_pubsub(event, context):
#    """Triggered from a message on a Cloud Pub/Sub topic.
#    Args:
#         event (dict): Event payload.
#         context (google.cloud.functions.Context): Metadata for the event.
#    """
#    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
def index():
    data = request.get_json()
    if not data:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(data, dict) or 'message' not in data:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = data['message']
    pubsub_message = base64.b64decode(pubsub_message['data']).decode('utf-8')

    print(pubsub_message)
    start_time = datetime.now()


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

    # TODO(developer)
    project_id = "fiery-marking-325513"
    topic_id = "downloaded_files"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = str(pubsub_message)
    # Data must be a bytestring
    data = data.encode("utf-8")
    # Add two attributes, origin and username, to the message
    future = publisher.publish(
        topic_path, data, origin="python-sample", username="gcp"
    )
    print(future.result())

    print(f"Published messages with custom attributes to {topic_path}.")

    return "Hello {}!".format(pubsub_message)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

