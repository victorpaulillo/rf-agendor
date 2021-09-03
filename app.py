import os
import base64
import requests
import datetime

from flask import Flask

app = Flask(__name__)

@app.route("/")
def download():
    start_time = datetime.datetime.now()
    pubsub_message = 'F.K03200$Z.D10710.MOTICSV.zip'
    url = 'http://200.152.38.155/CNPJ/' + pubsub_message 
    myfile = requests.get(url)
    down_time = datetime.datetime.now()

    open('downloads/' + pubsub_message, 'wb').write(myfile.content)
    download_time = datetime.datetime.now()

    print('Down time: ', down_time - start_time)
    print('Download time: ', download_time - start_time)
    print('terminei de baixar')
        
    return "Hello {}!".format(pubsub_message)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

