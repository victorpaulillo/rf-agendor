from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


from google.cloud import bigquery
import pendulum



#-------------------------------------------------------------------------------
# Settings 
default_args = {
 'owner': 'Victor Paulillo',
 'retries': 5,
 'retry_delay': timedelta(minutes=5),
 'email': ['victor.paulilllo@gmail.com'],
 'email_on_failure': True
}

local_tz = pendulum.timezone('America/Sao_Paulo')

#--------------------------------------------------------------------------------

dag = DAG(
    dag_id='dag_rf_cnpj',
    description=f'DAG para fazer o ETL e load dos dados de CNPJ da Receita Federal para uso da API de Cadastro do Agendor',
    schedule_interval=None,
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
)

def bigquery_execution(query):
    client = bigquery.Client()
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Waits for job to complete.
    print(f"Query executed: {query}")

# def bigquery_to_postgres():
#     # from sqlalchemy import create_engine
#     # Construct a BigQuery client object.
#     bqclient = bigquery.Client()
#     table_name = 'rf-agendor.rf.rf_agendor_cadastro_api'
#     query = """
#         SELECT *
#         FROM {}
#         limit 100
#         """.format(table_name)
#     print(query)

#     print("The query data:")
#     df = (
#         bqclient.query(query)
#         .result()
#         .to_dataframe(
#             # Optionally, explicitly request to use the BigQuery Storage API. As of
#             # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
#             # API is used by default.
#             create_bqstorage_client=True,
#         )
#     )
#     print(df.head())
    

#     cnpj_rf/bigquery_to_postgres
#     engine = create_engine('postgresql://postgres:postgres@35.247.200.226:5432/rf')
#     df.to_sql('rf_agendor_cadastro_api_tmp', engine)

def bigquery_to_storage():
    from google.cloud import bigquery
    client = bigquery.Client()
    bucket_name = 'cnpj_rf/bigquery_to_postgres'
    project = "rf-agendor"
    dataset_id = "rf"
    table_id = "rf_agendor_cadastro_api"

    destination_uri = "gs://{}/{}".format(bucket_name, "rf_agendor_cadastro_api-*.csv")
    # destination_uri = ["gs://{}/{}".format(bucket_name, "rf_agendor_cadastro_api-1.csv"), "gs://{}/{}".format(bucket_name, "rf_agendor_cadastro_api-2.csv"), "gs://{}/{}".format(bucket_name, "rf_agendor_cadastro_api-3.csv")]]
    # destination_uri = "gs://{}/{}".format(bucket_name, "rf_agendor_cadastro_api.csv")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="southamerica-east1",
        job_config=job_config,
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )


def compose_file():
    from google.cloud import storage

    """Concatenate source blobs into destination blob."""
    bucket_name = 'cnpj_rf'

    first_blob_name = "bigquery_to_postgres/rf_agendor_cadastro_api-000000000001.csv"
    second_blob_name = "bigquery_to_postgres/rf_agendor_cadastro_api-000000000002.csv"
    destination_blob_name = "bigquery_to_postgres/rf_agendor_cadastro_api_composed.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "text/plain"

    # sources is a list of Blob instances, up to the max of 32 instances per request
    sources = [bucket.get_blob(first_blob_name), bucket.get_blob(second_blob_name)]
    destination.compose(sources)

    print(
        "New composite object {} in the bucket {} was created by combining {} and {}".format(
            destination_blob_name, bucket_name, first_blob_name, second_blob_name
        )
    )
    return destination


# # Define function using copy_from_dataFile to insert the dataframe.
# def copy_from_dataFile():
#     import psycopg2
#     from google.cloud import storage
# #  Here we are going save the dataframe on disk as a csv file, load # the csv file and use copy_from() to copy it to the table
#     # tmp_df = '../Learn Python Data Access/iris_temp.csv'
#     # df.to_csv(tmp_df, header=False,index = False)
#     # f = open(tmp_df, 'r')
    
#     client = storage.Client()
#     # get bucket with name
#     bucket = client.get_bucket('cnpj_rf')
#     # get bucket data as blob
#     blob = bucket.get_blob('bigquery_to_postgres/bigquery_to_postgres_rf_agendor_cadastro_api-000000000000_v.csv')
#     # convert to string
#     csv_file = blob.download_as_string()
#     table = 'rf_agendor_cadastro_api_tmp'
#     conn = psycopg2.connect(
#         host="35.247.200.226",
#         database="rf",
#         user="postgres",
#         password="postgres")
            
#     cursor = conn.cursor()
#     try:
#         cursor.copy_from(csv_file, table, sep=",")
#         print("Data inserted using copy_from_datafile() successfully....")
#     except (Exception, psycopg2.DatabaseError) as err:
#         # pass exception to function
#         print(err)
#         cursor.close()



# copy_from_dataFile = PythonOperator(
#     task_id='copy_from_dataFile',
#     dag=dag,
#     python_callable=copy_from_dataFile,
#     provide_context=True

#     )




# def storage_to_postgres():
#     import requests
#     project_id = ' rf-agendor'
#     instance_id = 'rf-agendor'
#     endpoint = 'https://sqladmin.googleapis.com/v1/projects/{project_id}/instances/{instance_id}/import'.format(project_id=project_id, instance_id=instance_id)
                
#     # data to be sent to api
#     data = {
#             "importContext":
#             {
#                 "fileType": "CSV",
#                 "uri": "gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000001.csv",
#                 "database": "rf",
#                 "csvImportOptions":
#                 {
#                     "table": "table_name"
#                 }
#             }
#             }
#     # sending post request and saving response as response object
#     r = requests.post(url = endpoint, data = data)
#     print(r.text)



# create table rf_agendor_cadastro_api_tmp_3
#     (
#     cnpj VARCHAR ,
#     matriz_filial VARCHAR,
#     nome_fantasia VARCHAR,
#     desc_situacao_cadastral VARCHAR,
#     data_situacao_cadastral VARCHAR,
#     data_inicio_atividade VARCHAR,
#     cnae VARCHAR,
#     nome_cnae_principal VARCHAR,
#     cnae_fiscal_secundaria VARCHAR,
#     logradouro VARCHAR,
#     numero VARCHAR,
#     complemento VARCHAR,
#     bairro VARCHAR,
#     cep VARCHAR,
#     uf VARCHAR,
#     nome_municipio VARCHAR,
#     ddd_1 VARCHAR,
#     telefone_1 VARCHAR,
#     ddd_2 VARCHAR,
#     telefone_2 VARCHAR,
#     correio_eletronico VARCHAR,
#     porte VARCHAR,
#     razao_social VARCHAR,
#     capital_social VARCHAR,
#     natureza_juridica VARCHAR,
#     cnpj_basico VARCHAR,
#     socios_json VARCHAR
# );

# gcloud sql import csv rf-agendor gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000001.csv --database=rf --table=rf_agendor_cadastro_api_tmp ;



def list_storage_files():
    from google.cloud import storage

    client = storage.Client()
    list_files = []
    for blob in client.list_blobs('cnpj_rf', prefix='bigquery_to_postgres'):
        print(str(blob))
        blob_name = blob.name
        list_files.append(blob_name)
    len_list_files = len(list_files)

    return list_files, len_list_files


# list_storage_files = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]

# Function to join the files found on Google Cloud Storage and add it on one string bash command
# def storage_to_postgres_bash_command(**kwargs):
#     import subprocess
#     # ti = kwargs['ti']
#     # number = kwargs.get('number')
#     # list_files = ti.xcom_pull(task_ids='bq_to_postgres_files')
#     # file = list_files[0][int(number)]
#     # file_name = 'gs://cnpj_rf/' + file
#     # file_name = kwargs.get('file_name')
#     database='rf'
#     table='rf_agendor_cadastro_api_tmp'
#     file_name='gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000019.csv'

#     gcloud_import_command = 'gcloud sql import csv rf-agendor {} --database={} --table={} ; '.format(file_name, database, table)

#     print(gcloud_import_command)
    
#     bashCommand = gcloud_import_command
#     process = subprocess.Popen(bashCommand, shell = True, stdout=subprocess.PIPE)
#     output, error = process.communicate()
    
#     print(output)
#     print(error)

#     print('Completed loading the file {} on postgres at database={} and table={}'.format(file_name, database, table))
#     return output, error

def storage_to_postgres_bash_command(**kwargs):
    from pprint import pprint

    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials

    ti = kwargs['ti']
    number = kwargs.get('number')
    print(number)
    list_files = ti.xcom_pull(task_ids='list_storage_files')
    print(list_files)
    file = list_files[0][int(number)]
    print(file)
    file_name = 'gs://cnpj_rf/' + file
    print(file_name)
    # file_name = kwargs.get('file_name')
    # print(file_name)
    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    # Project ID of the project that contains the instance.
    project = 'rf-agendor'  # TODO: Update placeholder value.

    # Cloud SQL instance ID. This does not include the project ID.
    instance = 'rf-agendor'  # TODO: Update placeholder value.
    table='rf_agendor_cadastro_api_tmp_{}'.format(number)
    # file_name='gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000021.csv'


    instances_import_request_body = {
        
                "importContext":
                {
                    "fileType": "CSV",
                    # "uri": "gs://cnpj_rf/{file_name}".format(file_name=file_name),
                    "uri": file_name,
                    "database": "rf",
                    "csvImportOptions":
                    {
                        "table": "{table}".format(table=table)
                }
                }
    }

    print(instances_import_request_body)
    request = service.instances().import_(project=project, instance=instance, body=instances_import_request_body)
    response = request.execute()

    # TODO: Change code below to process the `response` dict:
    pprint(response)
    return response



# def append_storage_files(**kwargs):
#     import subprocess

#     gcloud_append_files = 'gsutil cat gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000000.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000001.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000002.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000003.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000004.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000005.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000006.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000007.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000008.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000009.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000010.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000011.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000012.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000013.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000014.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000015.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000016.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000017.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000018.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000019.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000020.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000021.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000022.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000023.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000024.csv gs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000025.csvgs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000026.csvgs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000027.csvgs://cnpj_rf/bigquery_to_postgres/rf_agendor_cadastro_api-000000000028.csv> lala.csv'

#     print(gcloud_append_files)
    
#     process = subprocess.Popen(gcloud_append_files, shell = True, stdout=subprocess.PIPE)
#     output, error = process.communicate()
#     print(output)
#     print(error)
#     print('Completed appending the files')


#     gcloud_copping_appended_file_storage = 'gsutil cp lala.csv gs://cnpj_rf/bigquery_to_postgres/lala.csv'

#     process = subprocess.Popen(gcloud_copping_appended_file_storage, shell = True, stdout=subprocess.PIPE)
#     output, error = process.communicate()
#     print(output)
#     print(error)
#     print('Completed coping the appended file to storage')


#     return 'Success!'

# append_storage_files = PythonOperator(
#     task_id='append_storage_files',
#     dag=dag,
#     python_callable=append_storage_files,
#     provide_context=True,
#     # op_kwargs={'data': "{{ ti.xcom_pull(task_ids='bq_to_postgres_files') }}", "number": "1" }
#     )

bigquery_to_storage = PythonOperator(
    task_id='bigquery_to_storage',
    dag=dag,
    python_callable=bigquery_to_storage,
    provide_context=True

    )

compose_file
compose_file = PythonOperator(
    task_id='compose_file',
    dag=dag,
    python_callable=compose_file,
    provide_context=True

    )

storage_to_postgres_bash_command_1 = PythonOperator(
    task_id='storage_to_postgres_bash_command_1',
    dag=dag,
    python_callable=storage_to_postgres_bash_command,
    provide_context=True,
    op_kwargs={'data': "{{ ti.xcom_pull(task_ids='list_storage_files') }}", "number": "1" }

    )


storage_to_postgres_bash_command_2 = PythonOperator(
    task_id='storage_to_postgres_bash_command_2',
    dag=dag,
    python_callable=storage_to_postgres_bash_command,
    provide_context=True,
    op_kwargs={'data': "{{ ti.xcom_pull(task_ids='list_storage_files') }}", "number": "2" }

    )
    
storage_to_postgres_bash_command_3 = PythonOperator(
    task_id='storage_to_postgres_bash_command_3',
    dag=dag,
    python_callable=storage_to_postgres_bash_command,
    provide_context=True,
    op_kwargs={'data': "{{ ti.xcom_pull(task_ids='list_storage_files') }}", "number": "3" }

    )

# storage_to_postgres_bash_command_v2 = PythonOperator(
#     task_id='storage_to_postgres_bash_command_v2',
#     dag=dag,
#     python_callable=storage_to_postgres_bash_command_v2,
#     provide_context=True,
#     # op_kwargs={'data': "{{ ti.xcom_pull(task_ids='bq_to_postgres_files') }}", "number": "7" }

#     )

# # xcom_get_import_command = '{{ ti.xcom_pull(task_ids="storage_to_postgres_bash_command")}}' 



# # BashOperator to import the files from the GCS to Postgres stage table. It runs the bash command string with the multiple files
# import_files_stage = BashOperator(
#     task_id="import_files_stage",
#     bash_command=xcom_get_import_command,
#     retries=2,
#     retry_delay=timedelta(minutes=2),
#     dag=dag
# )


# def values_function():
#     values = [0,1,2,3,4,5]
#     return values

# push_func = PythonOperator(
#         task_id='push_func',
#         provide_context=True,
#         python_callable=values_function,
#         dag=dag)

# def group_bash_command(number, **kwargs):
#         print('lala')
#         #load the values if needed in the command you plan to execute
#         file_number = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
#         print('lala')
#         return PythonOperator(task_id='remove_spec_char_{}'.format(str(number)),
#             dag=dag,python_callable=storage_to_postgres_bash_command,
#             op_kwargs={"file_number":str(number)}
#             )
        
# def test(i):
#     next = ' >> '
#     return (group_bash_command(i), next)
        
# for i in values_function():
#         push_func >> test(i) 





# Run the function that join on one string the files found on Google Cloud Storage
# gcs_files_create_bash_command = PythonOperator(
#     task_id='gcs_files_create_bash_command',
#     dag=dag,
#     python_callable=gcs_files_create_bash_command,
#     provide_context=True,  
#     templates_dict={'gcs_path_filename': "{{ ti.xcom_pull(task_ids='gcs_files') }}" },
#     xcom_push=True,
#     )

# # Get the result from the join files function
# xcom_get_import_command = '{{ ti.xcom_pull(task_ids="gcs_files_create_bash_command")}}'

# #BashOperator to import the files from the GCS to Postgres stage table. It runs the bash command string with the multiple files
# import_files_stage = BashOperator(
#     task_id="import_files_stage",
#     bash_command=xcom_get_import_command,
#     retries=2,
#     retry_delay=timedelta(minutes=2),
#     dag=dag
# )



    
ct_qualificacoes_socios_query = """
    create external table if not exists `rf-agendor.rf.qualificacoes_socios`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.QUALSCSV']
    );
    """

ct_paises_query = """
    create external table if not exists `rf-agendor.rf.paises`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.PAISCSV']
    );
"""

ct_natureza_juridica_query = """
    create external table if not exists `rf-agendor.rf.natureza_juridica`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.NATJUCSV']
    );
    """

ct_municipios_query = """
    create external table if not exists `rf-agendor.rf.municipios`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.MUNICCSV']
    );
"""

ct_empresas_query = """
    create external table if not exists `rf-agendor.rf.empresas`
    (
    cnpj_basico string,
    razao_social string,
    natureza_juridica string,
    qualificacao_responsavel string,
    capital_social string,
    porte_empresa string,
    ente_federativo_responsavel string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.EMPRECSV']
    );
    """

ct_cnae_query = """
    create external table if not exists `rf-agendor.rf.cnae`
    (
    codigo	STRING,
    descricao	STRING
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.CNAECSV']
    );
    """

ct_estabelecimentos_query = """
    create external table if not exists `rf-agendor.rf.estabelecimentos`
    (
        cnpj_basico STRING , 
        cnpj_ordem STRING , 
        cnpj_dv STRING , 
        identificador_matriz_filial STRING , 
        nome_fantasia STRING , 
        situacao_cadastral STRING , 
        data_situacao_cadastral STRING , 
        motivo_situacao_cadastral STRING , 
        nome_cidade_exterior STRING , 
        pais STRING , 
        data_inicio_atividade STRING , 
        cnae_fiscal_principal STRING , 
        cnae_fiscal_secundaria STRING , 
        tipo_logradouro STRING , 
        logradouro STRING , 
        numero STRING , 
        complemento STRING , 
        bairro STRING , 
        cep STRING , 
        uf STRING , 
        municipio STRING , 
        ddd_1 STRING , 
        telefone_1 STRING , 
        ddd_2 STRING , 
        telefone_2 STRING , 
        ddd_fax STRING , 
        fax STRING , 
        correio_eletronico STRING , 
        situacao_especial STRING , 
        data_situacao_especial STRING
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files_treated/*.ESTABELE']
    );
    """

ct_socios_query = """
    create external table if not exists `rf-agendor.rf.socios`
    (
    cnpj_basico	STRING,
    identificador_socio	STRING,
    nome_socio	STRING,
    cpf_cnpj_socio	STRING,
    qualificacao_socio	STRING,
    data_entrada_sociedade	STRING,
    pais	STRING,
    representante_legal	STRING,
    nome_representante	STRING,
    qualificacao_representante	STRING,
    faixa_etaria	STRING
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf/unzip_files/*.SOCIOCSV']
    );
    """

ct_socios_agg_json_query = """
    create or replace table 	`rf-agendor.rf.socios_agg_json`
    (
    cnpj_basico STRING,
    socios_json ARRAY<STRING>
    );
    """

ct_rf_agendor_cadastro_api_query = """
    create or replace table 	`rf-agendor.rf.rf_agendor_cadastro_api`
    (
    cnpj	STRING,
    matriz_filial	STRING,
    nome_fantasia	STRING,
    desc_situacao_cadastral	STRING,
    data_situacao_cadastral	DATE,
    data_inicio_atividade	DATE,
    cnae	STRING,
    nome_cnae_principal	STRING,
    cnae_fiscal_secundaria	STRING,
    logradouro	STRING,
    numero	STRING,
    complemento	STRING,
    bairro	STRING,
    cep	STRING,
    uf	STRING,
    nome_municipio	STRING,
    ddd_1	STRING,
    telefone_1	STRING,
    ddd_2	STRING,
    telefone_2	STRING,
    correio_eletronico	STRING,
    porte	STRING,
    razao_social	STRING,
    capital_social	STRING,
    natureza_juridica	STRING,
    cnpj_basico	STRING,
    --socios_json ARRAY<STRING>
    socios_json STRING
    );
    """

insert_into_socios_agg_json_query = """
    insert into `rf-agendor.rf.socios_agg_json`
    with 
    socios as (
    select s.cnpj_basico
    , case when s.identificador_socio = '1' then 'PESSOA JURÍDICA'
        when s.identificador_socio = '2' then 'PESSOA FÍSICA'
        when s.identificador_socio = '3' then 'ESTRANGEIRO'
        else null
        end as identificador_socio
    , nome_socio
    , cpf_cnpj_socio
    , replace(q.descricao, '�', 'ç')  as qualificacao_socio
    , case when s.data_entrada_sociedade = '0' then null else SAFE.PARSE_DATE('%Y%m%d', s.data_entrada_sociedade) end as data_entrada_sociedade
    , p.descricao as pais
    , s.representante_legal as cpf_representante_legal
    , replace(q_representante.descricao, '�', 'ç') as qualificacao_representante
    , case when s.faixa_etaria = '1' then 'Entre 0 a 12 anos'
            when s.faixa_etaria = '2' then 'Entre 13 a 20 anos'
            when s.faixa_etaria = '3' then 'Entre 21 a 30 anos'
            when s.faixa_etaria = '4' then 'Entre 31 a 40 anos'
            when s.faixa_etaria = '5' then 'Entre 41 a 50 anos'
            when s.faixa_etaria = '6' then 'Entre 51 a 60 anos'
            when s.faixa_etaria = '7' then 'Entre 61 a 70 anos'
            when s.faixa_etaria = '8' then 'Entre 71 a 80 anos'
            when s.faixa_etaria = '9' then 'Maiores de 80 anos'
            when s.faixa_etaria = '0' then 'Não se aplica'
        end as faixa_etaria
    
    from rf.socios as s
    left join rf.qualificacoes_socios as q
    on s.qualificacao_socio = q.codigo
    left join rf.qualificacoes_socios as q_representante
    on s.qualificacao_socio = q_representante.codigo
    left join rf.paises as p
    on s.pais = p.codigo
    )

    select cnpj_basico
    , ARRAY_AGG(TO_JSON_STRING(s)) AS socios_json 
    from socios as s  --  6.865.797
    group by cnpj_basico
    order by cnpj_basico
    ;
    """

insert_into_rf_agendor_cadastro_api_query = """
insert into `rf-agendor.rf.rf_agendor_cadastro_api`
select e.cnpj_basico || cnpj_ordem || cnpj_dv as cnpj
  , case when e.identificador_matriz_filial = '1' then 'Matriz'
      when e.identificador_matriz_filial = '2' then 'Filial'
      else '' end as matriz_filial
  , nome_fantasia
  , case when situacao_cadastral = '01' then 'Nula'
      when situacao_cadastral = '02' then 'Ativa'
      when situacao_cadastral = '03' then 'Suspensa'
      when situacao_cadastral = '04' then 'Inapta'
      when situacao_cadastral = '08' then 'Baixada'
      end as desc_situacao_cadastral
   , case when data_situacao_cadastral = '0' then null else SAFE.PARSE_DATE('%Y%m%d', data_situacao_cadastral) end as data_situacao_cadastral
   , case when data_situacao_cadastral = '0' then null else SAFE.PARSE_DATE('%Y%m%d', data_inicio_atividade) end as data_inicio_atividade
   , cnae_fiscal_principal as cnae
  , replace(c.descricao, '�', 'ç') as nome_cnae_principal
  , cnae_fiscal_secundaria
  , logradouro
  , numero
  , complemento
  , bairro
  , cep
  , uf
  , m.descricao as nome_municipio
  , ddd_1
  , telefone_1
  , ddd_2
  , telefone_2
  , correio_eletronico
  , case when emp.porte_empresa = '01' then 'NAO INFORMADO'
          when emp.porte_empresa = '02' then 'MICRO EMPRESA'
          when emp.porte_empresa = '03' then 'EMPRESA DE PEQUENO PORTE'
          when emp.porte_empresa = '05' then 'DEMAIS'
    end as porte
  , emp.razao_social
  , emp.capital_social
  , replace(n.descricao, '�', 'ç') as natureza_juridica
  , s.cnpj_basico
  --, s.socios_json
  ,  ARRAY_TO_STRING(s.socios_json, ',') as socios_json

from rf.estabelecimentos as e
left join rf.empresas as emp
on emp.cnpj_basico = e.cnpj_basico
left join rf.cnae as c
on e.cnae_fiscal_principal = c.codigo
left join rf.municipios as m
on e.municipio = m.codigo
left join rf.natureza_juridica as n
on n.codigo = emp.natureza_juridica
left join rf.socios_agg_json as s
on s.cnpj_basico = e.cnpj_basico

"""

ct_qualificacoes_socios = PythonOperator(task_id='ct_qualificacoes_socios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_qualificacoes_socios_query})
ct_paises = PythonOperator(task_id='ct_paises',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_paises_query})
ct_natureza_juridica = PythonOperator(task_id='ct_natureza_juridica',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_natureza_juridica_query})
ct_municipios = PythonOperator(task_id='ct_municipios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_municipios_query})
ct_empresas = PythonOperator(task_id='ct_empresas',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_empresas_query})
ct_cnae = PythonOperator(task_id='ct_cnae',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_cnae_query})
ct_estabelecimentos = PythonOperator(task_id='ct_estabelecimentos',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_estabelecimentos_query})
ct_socios = PythonOperator(task_id='ct_socios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_socios_query})

ct_socios_agg_json = PythonOperator(task_id='ct_socios_agg_json',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_socios_agg_json_query})
ct_rf_agendor_cadastro_api = PythonOperator(task_id='ct_rf_agendor_cadastro_api',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_rf_agendor_cadastro_api_query})

insert_into_socios_agg_json = PythonOperator(task_id='insert_into_socios_agg_json',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":insert_into_socios_agg_json_query})
insert_into_rf_agendor_cadastro_api = PythonOperator(task_id='insert_into_rf_agendor_cadastro_api',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":insert_into_rf_agendor_cadastro_api_query})

list_storage_files = PythonOperator(task_id='list_storage_files',dag=dag,python_callable=list_storage_files)



# storage_to_postgres_0 = PythonOperator(task_id='storage_to_postgres_0',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"0"})
# storage_to_postgres_bash_command_1 = PythonOperator(task_id='storage_to_postgres_bash_command_1',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"1"})
# storage_to_postgres_bash_command_2 = PythonOperator(task_id='storage_to_postgres_bash_command_2',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"2"})
# storage_to_postgres_bash_command_3 = PythonOperator(task_id='storage_to_postgres_bash_command_3',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"3"})
# storage_to_postgres_bash_command_4 = PythonOperator(task_id='storage_to_postgres_bash_command_4',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"4"})
# storage_to_postgres_bash_command_5 = PythonOperator(task_id='storage_to_postgres_bash_command_5',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"5"})
# storage_to_postgres_bash_command_6 = PythonOperator(task_id='storage_to_postgres_bash_command_6',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"6"})
# storage_to_postgres_bash_command_7 = PythonOperator(task_id='storage_to_postgres_bash_command_7',dag=dag,python_callable=storage_to_postgres_bash_command,op_kwargs={"number":"7"})

# storage_to_postgres = PythonOperator(task_id='storage_to_postgres',dag=dag,python_callable=storage_to_postgres)

start_dag = DummyOperator(task_id='start_dag', dag=dag)
create_external_tables = DummyOperator(task_id='create_external_tables', dag=dag)
create_tables = DummyOperator(task_id='create_tables', dag=dag)
insert_records = DummyOperator(task_id='insert_records', dag=dag)
storage_upload_files = DummyOperator(task_id='storage_upload_files', dag=dag)

#Workflow

start_dag >> create_external_tables >> [ct_qualificacoes_socios, ct_paises, ct_natureza_juridica, ct_municipios, ct_empresas, ct_cnae, ct_estabelecimentos, ct_socios] >> insert_records
start_dag >> create_tables >> [ct_socios_agg_json, ct_rf_agendor_cadastro_api] >> insert_records

insert_records >> insert_into_socios_agg_json >> insert_into_rf_agendor_cadastro_api  >> bigquery_to_storage >> list_storage_files >> storage_upload_files 

storage_upload_files >> [storage_to_postgres_bash_command_1, storage_to_postgres_bash_command_2, storage_to_postgres_bash_command_3]
# , storage_to_postgres_bash_command_1, storage_to_postgres_bash_command_2, storage_to_postgres_bash_command_3, storage_to_postgres_bash_command_4, storage_to_postgres_bash_command_5, storage_to_postgres_bash_command_6, storage_to_postgres_bash_command_7]
# >> storage_to_postgres





# def remove_spec_char(**kwargs):
#     ti = kwargs['ti']
#     file_number = kwargs.get('file_number')
#     bucket_name = kwargs.get('bucket_name')
#     file_name_ = ti.xcom_pull(task_ids='list_files_rf')[int(file_number)]

#     if file_name_ == '':
#         None

#     else:
#         if 'ESTABELE' in file_name_:

#             file_name_final = file_name_[:-4]
#             blob_name = "unzip_files/" + file_name_final
#             new_blob_special_character = "unzip_files_treated/" + file_name_final

#             client = storage.Client()
#             bucket = client.get_bucket(bucket_name)

#             blob = bucket.get_blob(blob_name)
#             file = blob.download_as_string()

#             file_decoded = file.decode("ISO-8859-1")
#             file_upload = re.sub(r"[^a-zA-Z0-9,-@+_ \"\n]", '', str(file_decoded))
            
#             bucket.blob(new_blob_special_character).upload_from_string(file_upload, 'text/csv')
            
#             print(f'File moved from {blob_name} to {new_blob_special_character}')

#         else:
#             print(f'Did nothing, file is not ESTABELE, it is {file_name_}')
