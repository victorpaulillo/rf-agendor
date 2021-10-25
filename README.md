# rf-agendor

#Create a venv
python -m venv ./venv/

# Activate the venv
.\venv\Scripts\activate

# See list
pip list

# Install Flask and libs
pip install flask
pip install requests


# Create a requirements.txt
pip freeze > requirements.txt

# Create a .gitignore file - Tell git what do ignore

# Run flask
flask run


# To deploy the container on Cloud Run
gcloud run deploy

# To run locally, it is necessary to change the name of the main scrip to "app.py"

Kubernetes
https://medium.com/google-cloud/a-guide-to-deploy-flask-app-on-google-kubernetes-engine-bfbbee5c6fb


# Update the Dockerfile according to the medium post

# Build the flask app using the code:
gcloud builds --project fiery-marking-325513\
    submit --tag gcr.io/fiery-marking-325513/flask-app:v1 .

# flask-app:v1 






# README - Infra Airflow

https://www.youtube.com/watch?v=aTaytcxy2Ck
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

sudo apt-get update
sudo apt install docker.io

-- Docker compose - https://docs.docker.com/compose/install/
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
docker-compose --version

mkdir airflow-docker
cd airflow-docker
-- curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.1.0/docker-compose.yaml"
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
sudo docker-compose up airflow-init
sudo docker-compose up

Create a firewall rule Open VPC Network > Firewall Rules Create Firewall rules Name: airflow-ingress Priority: 8080 Targets: All instances in the network Source IP ranges: 0.0.0.0/0 Protocols and ports > Check tcp box

