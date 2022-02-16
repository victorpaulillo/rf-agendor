# rf-agendor

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
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.1.0/docker-compose.yaml"
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
sudo docker-compose up airflow-init
sudo docker-compose up

Create a firewall rule Open VPC Network > Firewall Rules Create Firewall rules Name: airflow-ingress Priority: 8080 Targets: All instances in the network Source IP ranges: 0.0.0.0/0 Protocols and ports > Check tcp box

Liberar permissao da Conta de Serviço da Instancia do Postgres no bucket do Google Cloud Storage

git clone https://github.com/victorpaulillo/rf-agendor.git

Instalar as bibliotecas nos dockers:
sudo docker exec d218f3a86859         pip install subprocess.run
sudo docker exec c4b2b9553bb7         pip install subprocess.run
sudo docker exec b14e0b08baa9         pip install subprocess.run
sudo docker exec 080d76b486f9         pip install subprocess.run

sudo docker exec d218f3a86859         pip install pandas
sudo docker exec c4b2b9553bb7         pip install pandas
sudo docker exec b14e0b08baa9         pip install pandas
sudo docker exec 080d76b486f9         pip install pandas

sudo docker exec d218f3a86859         pip install oauth2client
sudo docker exec c4b2b9553bb7         pip install oauth2client
sudo docker exec b14e0b08baa9         pip install oauth2client
sudo docker exec 080d76b486f9         pip install oauth2client

sudo docker exec d218f3a86859         pip install BeautifulSoup4
sudo docker exec c4b2b9553bb7         pip install BeautifulSoup4
sudo docker exec b14e0b08baa9         pip install BeautifulSoup4
sudo docker exec 080d76b486f9         pip install BeautifulSoup4

Add Sinalizador no Postgres:
temp_file_limit: 155055030

Create 'rf' schema on biguery (Needs to be on the same region as the storage bucket)
Remember to add permissions to the users to access the data (could happend an issue related to that)

Enalbe Cloud SQL API Admin https://console.cloud.google.com/apis/library/sqladmin.googleapis.com?project=rf-agendor-335020

Add the Public IP to be able to connect to Cloud SQL Postgres - Connection -> Public 0.0.0.0/0


Create a environment variable on linux with the password of database with the name "DB_PASS"
Follow the command below on SSH of Airflow Machine
DB_PASS='password'

------------------------------------------------------------------------------------


4. Colocar a função nos padroes da API REST 
5. CORS autorização - OK
11. Tentar ao inves de rename, criar uma hash com os valores de todos os campos. E ai comparar a hash da tabela tmp com a tabela de prod e em seguida inserir os dados que a hash for diferente
3. Colocar um operador no airflow de listener para quando terminar o comando de inserir o arquivo no postgres. Se possivel a api tem que jogar uma mensagem em uma fila apos terminar.


# Pontos de atenção
E vou mandar aqui uns pontos de atenção:
- O porte vem incorreto varias vezes. Aparece muitas vezes NAO INFORMADO, mas o cara tem porte ME, ou outros...
- O logradouro (endereço) aparece sem o "Rua", "Avenida", "Rodovia", ou qualquer outro prefixo que informe o "tipo de rua". Pode ser algum erro de uma transformação minha. Preciso validar, mas acho que é assim que o dado bem mesmo. Ex.: No site da receita "JD IRACEMA" na base da receita, que estou disponibilizando "IRACEMA"
- Quando a natureza_juridica é igual "Empresário (Individual)" não vai ter quadro de sócios. Isso está igual ao o que aparece no site da receita consultando na mão

