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


Liberar permissao da Conta de Serviço da Instancia do Postgres no bucket do Google Cloud Storage


O que fazer:
1. Jogar os dados para o Postgres - Tabela Temp, ai mudar o nome para producao e o da tabela de producao mudar para backup
2. Formato da data yyy-mm-dd
3. Converter na api o campo de json de string para json mesmo. Tirar as barrrinhas 
4. Colocar a função nos padroes da API REST 
5. CORS autorização - OK
6. Retirar um monte de codigo repetido por jeito dinamico de gerar operadores: https://stackoverflow.com/questions/41517798/proper-way-to-create-dynamic-workflows-in-airflow
7. Criar as tabelas para criação de ETL dentro do airflow em uma nova dag, que a dag principal trigga. OK
8. Copiar os dados do bigquery para uma tabela temp no postgres. Ai contar a quantidade de registros dessa tabela temp, se for mais ou igual a tabela de prod, ai faz o rename na de prod, se nao da um alerta
9. Fazer ukm looping paralelo para importar arquivo por arquivo, cada um em uma caixinha, assim se der erro, ele tenta novamente


# Pontos de atenção
E vou mandar aqui uns pontos de atenção:
- O porte não vem incorreto varias vezes. Aparece muitas vezes NAO INFORMADO, mas o cara tem porte ME, ou outros...
- O logradouro (endereço) aparece sem o "Rua", "Avenida", "Rodovia", ou qualquer outro prefixo que informe o "tipo de rua". Pode ser algum erro de uma transformação minha. Preciso validar, mas acho que é assim que o dado bem mesmo. Ex.: No site da receita "JD IRACEMA" na base da receita, que estou disponibilizando "IRACEMA"
- Quando a natureza_juridica é igual "Empresário (Individual)" não vai ter quadro de sócios. Isso está igual ao o que aparece no site da receita consultando na mão




https://stackoverflow.com/questions/58901438/how-do-i-efficiently-migrate-the-bigquery-tables-to-on-prem-postgres