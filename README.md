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
-- curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
curl -LfO "https://airflow.apache.org/docs/apache-airflow/2.1.0/docker-compose.yaml"
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
sudo docker-compose up airflow-init
sudo docker-compose up

Create a firewall rule Open VPC Network > Firewall Rules Create Firewall rules Name: airflow-ingress Priority: 8080 Targets: All instances in the network Source IP ranges: 0.0.0.0/0 Protocols and ports > Check tcp box

Liberar permissao da Conta de Serviço da Instancia do Postgres no bucket do Google Cloud Storage

git clone https://github.com/victorpaulillo/rf-agendor.git

Instalar as bibliotecas nos dockers:
sudo docker exec 543ca3d1c42c         pip install subprocess.run
sudo docker exec ae72b080b404         pip install subprocess.run
sudo docker exec 73abfba5c808         pip install subprocess.run
sudo docker exec e10272695a0d         pip install subprocess.run

sudo docker exec 543ca3d1c42c         pip install pandas
sudo docker exec ae72b080b404         pip install pandas
sudo docker exec 73abfba5c808         pip install pandas
sudo docker exec e10272695a0d         pip install pandas

sudo docker exec 543ca3d1c42c         pip install oauth2client
sudo docker exec ae72b080b404         pip install oauth2client
sudo docker exec 73abfba5c808         pip install oauth2client
sudo docker exec e10272695a0d         pip install oauth2client

sudo docker exec 543ca3d1c42c         pip install BeautifulSoup4
sudo docker exec ae72b080b404         pip install BeautifulSoup4
sudo docker exec 73abfba5c808         pip install BeautifulSoup4
sudo docker exec e10272695a0d         pip install BeautifulSoup4

Add Sinalizador no Postgres:
temp_file_limit: 155055030

Create 'rf' schema on biguery (Needs to be on the same regin as the storage bucket)
Remember to add permissions to the users to access the data (could happend an issue related to that)



------------------------------------------------------------------------------------



O que fazer:
1. Jogar os dados para o Postgres - Tabela Temp, ai mudar o nome para producao e o da tabela de producao mudar para backup
2. Formato da data yyy-mm-dd
3. Converter na api o campo de json de string para json mesmo. Tirar as barrrinhas 
4. Colocar a função nos padroes da API REST 
5. CORS autorização - OK
6. Retirar um monte de codigo repetido por jeito dinamico de gerar operadores: https://stackoverflow.com/questions/41517798/proper-way-to-create-dynamic-workflows-in-airflow
7. Criar as tabelas para criação de ETL dentro do airflow em uma nova dag, que a dag principal trigga. OK
8. Copiar os dados do bigquery para uma tabela temp no postgres. Ai contar a quantidade de registros dessa tabela temp, se for mais ou igual a tabela de prod, ai faz o rename na de prod, se nao da um alerta
9. Fazer um looping paralelo para importar arquivo por arquivo, cada um em uma caixinha, assim se der erro, ele tenta novamente

10. Trocar o drop prod table, to rename to backup
11. Tentar ao inves de rename, criar uma hash com os valores de todos os campos. E ai comparar a hash da tabela tmp com a tabela de prod e em seguida inserir os dados que a hash for diferente

# Pontos de atenção
E vou mandar aqui uns pontos de atenção:
- O porte não vem incorreto varias vezes. Aparece muitas vezes NAO INFORMADO, mas o cara tem porte ME, ou outros...
- O logradouro (endereço) aparece sem o "Rua", "Avenida", "Rodovia", ou qualquer outro prefixo que informe o "tipo de rua". Pode ser algum erro de uma transformação minha. Preciso validar, mas acho que é assim que o dado bem mesmo. Ex.: No site da receita "JD IRACEMA" na base da receita, que estou disponibilizando "IRACEMA"
- Quando a natureza_juridica é igual "Empresário (Individual)" não vai ter quadro de sócios. Isso está igual ao o que aparece no site da receita consultando na mão




https://medium.com/google-cloud/apache-airflow-how-to-add-a-connection-to-google-cloud-with-cli-af2cc8df138d

https://medium.com/analytics-vidhya/part-4-pandas-dataframe-to-postgresql-using-python-8ffdb0323c09




1. Trocar de SP para os US mais barato
2. Entender o proque ta com 780 GB de SSD e reduzir - Central 1
3. Colocar um operador no airflow de listener para quando terminar o comando de inserir o arquivo no postgres. Se possivel a api tem que jogar uma mensagem em uma fila apos terminar.

----
