# rf-agendor

# README - Infra Airflow

https://www.youtube.com/watch?v=aTaytcxy2Ck
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

# 1. Create a VM Instance with 16GB RAM, 6 cores vCPU and 20GB Storage
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
Copie o nome da Conta de Serviço da Instance do Cloud SQL Postgres. Vá no bucket criado do Cloud Storage e adiocione as roles "Leitor de bucket legado do Storage" e "Leitor de objeto legado do Storage" para essa conta de serviço


# 4. Create the BigQuery schema
Create 'rf' schema on biguery (Needs to be on the same region as the storage bucket)
E adicionar o role no IAM "BigQuery Data Editor", "BigQuery Job User" e "Usuário de sessão de leitura do BigQuery" para a conta de serviço da VM criada


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

sudo docker exec ff56a132f97c                         pip install subprocess.run
sudo docker exec 80f94c332bee                         pip install subprocess.run
sudo docker exec 99efeb23f7cc                         pip install subprocess.run
sudo docker exec 3f5cf2fa9085                         pip install subprocess.run
sudo docker exec 43c1511e9bae                         pip install subprocess.run

sudo docker exec ff56a132f97c                         pip install pandas
sudo docker exec 80f94c332bee                         pip install pandas
sudo docker exec 99efeb23f7cc                         pip install pandas
sudo docker exec 3f5cf2fa9085                         pip install pandas
sudo docker exec 43c1511e9bae                         pip install pandas

sudo docker exec ff56a132f97c                         pip install oauth2client
sudo docker exec 80f94c332bee                         pip install oauth2client
sudo docker exec 99efeb23f7cc                         pip install oauth2client
sudo docker exec 3f5cf2fa9085                         pip install oauth2client
sudo docker exec 43c1511e9bae                         pip install oauth2client

sudo docker exec ff56a132f97c                         pip install BeautifulSoup4
sudo docker exec 80f94c332bee                         pip install BeautifulSoup4
sudo docker exec 99efeb23f7cc                         pip install BeautifulSoup4
sudo docker exec 3f5cf2fa9085                         pip install BeautifulSoup4
sudo docker exec 43c1511e9bae                         pip install BeautifulSoup4


