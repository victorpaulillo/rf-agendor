import os
import base64
import requests
from datetime import datetime
from google.cloud import storage
from flask import Flask

app = Flask(__name__)

@app.route("/")
def download():
    start_time = datetime.now()
    # pubsub_message = 'F.K03200$Z.D10710.MOTICSV.zip'
    pubsub_message = 'K3241.K03200Y0.D10710.ESTABELE.zip'
    url = 'http://200.152.38.155/CNPJ/' + pubsub_message 
    myfile = requests.get(url)
    down_time = datetime.now()

    open(pubsub_message, 'wb').write(myfile.content)
    download_time = datetime.now()

    print('Down time: ', down_time - start_time)
    print('Download time: ', download_time - start_time)
    print('terminei de baixar')



    bucket_name = "cnpj-rf"
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

    return "Hello {}!".format(pubsub_message)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

