# rf-agendor

# README - Infra Airflow

https://www.youtube.com/watch?v=aTaytcxy2Ck
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

# 1. Create a VM Instance with 16GB RAM and 6 cores vCPU
# Run the commands below on the Instance SSH
sudo apt-get update
sudo apt install docker.io

# Docker compose - https://docs.docker.com/compose/install/
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
docker-compose --version

mkdir airflow-docker
cd airflow-docker
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml"
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

sudo docker-compose up airflow-init
sudo docker-compose up
# Inside the folder "dags" clone the git repo
git clone https://github.com/victorpaulillo/rf-agendor.git


# Configure the firewall rule to be able to access the Airflow Application with 'External IP:8080'
Create a firewall rule Open VPC Network > Firewall Rules Create Firewall rules 
Name: airflow-ingress 
Priority: 8080 
Targets: All instances in the network 
Source IP ranges: 0.0.0.0/0 
Protocols and ports > Check tcp box

Make sure that the Airflow Docker is up and healthy by accessing the 'External IP:8080' or running the command on VM SSH "sudo docker ps"
If the Docker is not healthy, then go to "cd airflow-docker" and run the commands "sudo docker-compose down" and "sudo docker-compose up"

# 2. Create the Storage bucket
Criar um bucket no Google Cloud Storage com o nome "importacao_rf_agendor" com a mesma localização da VM (São Paulo)
Vá para a script "dag_rf_download" aproximadamente na linha 38 'Configs' e substitua ou verifique se o nome do bucket é o mesmo entre o script e o bucket criado


# 3. Create a Cloud SQL Postgres Instance
Criar uma instancia no Postgres com o nome "rf" na regiao de São Paulo
    Add Sinalizador (Flag) no Postgres: temp_file_limit: 155055030
    Add the Public IP to be able to connect to Cloud SQL Postgres - Connection -> Public 0.0.0.0/0
Vá para a script "dag_rf_cnpj" aproximadamente na linha 25 'Configs' e substitua ou verifique se o nome do projeto é o mesmo entre o script e o projeto do GCP
Crie o usuario do Agendor dev no Cloud SQL - 'agendor-dev'
Enable Cloud SQL API Admin https://console.cloud.google.com/apis/library/sqladmin.googleapis.com
Crie o database "rf" no postgres atraves do Cloud Shell, usando o comando "create database rf;"

# Será necessário fazer uma autenticacao OAuth com o Google para ser possível disparar um post para iniciar a importacao do arquivo csv do storage para o postgres.
Para isso rode o comando no ssh da vm 'gcloud auth login';
Obtenha o url;
Adicione o parametro "&redirect_uri=urn:ietf:wg:oauth:2.0:oob:auto" conforme a 'Copiar/colar manualmente' da documentação "https://developers.google.com/identity/protocols/oauth2/native-app"
Faça login, permita o autenticador e copie e cole o 'C[odigo de Autorização' no terminal



# 4. Create the BigQuery schema
Create 'rf' schema on biguery (Needs to be on the same region as the storage bucket)
E adicionar o role no IAM "BigQuery Data Editor" e "BigQuery Job User" para a conta de serviço da VM criada


# 5. Adicione as credenciais do Cloud SQL no Google Secret Manager
Adicionar no Secret Manager do Google as credenciais do Banco de Dados postgres, com os nomes:
DB_HOST
DB_USER
DB_PASS

E adicionar o role no IAM "Secret Manager Secret Accessor" para a conta de serviço da VM criada

# 6. Adicione a permissao do VM Instance Schedule na Conta de Serviço
Adicione o role no IAM "Compute Instance Admin (v1)" para a conta de serviço da VM criada


# 7. Instalar as bibliotecas nos dockers:
Obtenha os nomes dos containers apache/airflow atraves do comando no SSH 'sudo docker ps', substitua o CONTAINER_ID abaixo e e instale as bibliotecas

sudo docker exec e62ed3964966                         pip install subprocess.run
sudo docker exec ee9fba202ad3                         pip install subprocess.run
sudo docker exec 88d32e1fa876                         pip install subprocess.run
sudo docker exec 437f739832d3                         pip install subprocess.run
sudo docker exec d363f241be35                         pip install subprocess.run

sudo docker exec e62ed3964966                         pip install pandas
sudo docker exec ee9fba202ad3                         pip install pandas
sudo docker exec 88d32e1fa876                         pip install pandas
sudo docker exec 437f739832d3                         pip install pandas
sudo docker exec d363f241be35                         pip install pandas

sudo docker exec e62ed3964966                         pip install oauth2client
sudo docker exec ee9fba202ad3                         pip install oauth2client
sudo docker exec 88d32e1fa876                         pip install oauth2client
sudo docker exec 437f739832d3                         pip install oauth2client
sudo docker exec d363f241be35                         pip install oauth2client

sudo docker exec e62ed3964966                         pip install BeautifulSoup4
sudo docker exec ee9fba202ad3                         pip install BeautifulSoup4
sudo docker exec 88d32e1fa876                         pip install BeautifulSoup4
sudo docker exec 437f739832d3                         pip install BeautifulSoup4
sudo docker exec d363f241be35                         pip install BeautifulSoup4

<!-- sudo docker exec e62ed3964966                         pip install google-api-python-client
sudo docker exec ee9fba202ad3                         pip install google-api-python-client
sudo docker exec 88d32e1fa876                         pip install google-api-python-client
sudo docker exec 437f739832d3                         pip install google-api-python-client
sudo docker exec d363f241be35                         pip install google-api-python-client -->


pip install --upgrade google-api-python-client
