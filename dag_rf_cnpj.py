from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import bigquery
import pendulum
import os

#-------------------------------------------------------------------------------
# Settings 
default_args = {
 'owner': 'Victor Paulillo',
 'retries': 5,
 'retry_delay': timedelta(minutes=5),
 'email': ['victor.paulillo@gmail.com'], # devs@agendor.com.br
 'email_on_failure': True
}

local_tz = pendulum.timezone('America/Sao_Paulo')


# Credentials
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')



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


def bigquery_to_storage():
    # First we will delete all files from storage, to not duplicate any file
    from google.cloud import storage
    bucket_name = 'cnpj_rf_agendor'
    directory_name = 'bigquery_to_postgres'

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # list all objects in the directory
    blobs = bucket.list_blobs(prefix=directory_name)
    for blob in blobs:
        blob.delete()

    print('All files deleted from folder {}'.format(directory_name))

    # Now we are loading the big query table into storage

    from google.cloud import bigquery
    client = bigquery.Client()
    bucket_file_name = 'cnpj_rf_agendor/bigquery_to_postgres'
    project = "rf-agendor-335020"
    dataset_id = "rf"
    table_id = "rf_agendor_cadastro_api"

    destination_uri = "gs://{}/{}".format(bucket_file_name, "rf_agendor_cadastro_api-*")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="us-east1",
        job_config=job_config,
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

def compose_file(**kwargs):
    ti = kwargs['ti']
    list_files = ti.xcom_pull(task_ids='list_storage_files')
    print(list_files)
    from google.cloud import storage

    """Concatenate source blobs into destination blob."""
    bucket_name = 'cnpj_rf_agendor'
    destination_blob_name = "bigquery_to_postgres/rf_agendor_cadastro_api_composed"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "application/octet-stream"

    sources = []
    for file in list_files[0]:
        bucket_file = bucket.get_blob(file)
        sources.append(bucket_file)

    destination.compose(sources)

    text = "New composite object {} in the bucket {} was created by combining files from Receita Federal".format(
            destination_blob_name, bucket_name)
    return text

def list_storage_files():
    from google.cloud import storage

    client = storage.Client()
    list_files = []
    for blob in client.list_blobs('cnpj_rf_agendor', prefix='bigquery_to_postgres'):
        print(str(blob))
        blob_name = blob.name
        list_files.append(blob_name)
    len_list_files = len(list_files)

    return list_files, len_list_files


def storage_to_postgres_bash_command(**kwargs):
    from pprint import pprint

    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials


    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    # Project ID of the project that contains the instance.
    project = 'rf-agendor-335020'  # TODO: Update placeholder value.

    # Cloud SQL instance ID. This does not include the project ID.
    instance = 'rf-agendor'  # TODO: Update placeholder value.
    # table='rf_agendor_cadastro_api_tmp_{}'.format(number)
    table='rf_agendor_cadastro_api_stage'
    file_name='gs://cnpj_rf_agendor/bigquery_to_postgres/rf_agendor_cadastro_api_composed'


    instances_import_request_body = {
        
                "importContext":
                {
                    "fileType": "CSV",
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

bigquery_to_storage = PythonOperator(
    task_id='bigquery_to_storage',
    dag=dag,
    python_callable=bigquery_to_storage,
    provide_context=True

    )

compose_file = PythonOperator(
    task_id='compose_file',
    dag=dag,
    python_callable=compose_file,
    provide_context=True

    )

storage_to_postgres_bash_command = PythonOperator(
    task_id='storage_to_postgres_bash_command',
    dag=dag,
    python_callable=storage_to_postgres_bash_command,
    provide_context=True,
    # op_kwargs={'data': "{{ ti.xcom_pull(task_ids='list_storage_files') }}", "number": "1" }

    )

def create_stage_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        drop_tmp_table = """drop table if exists rf_agendor_cadastro_api_stage;"""
        cur.execute(drop_tmp_table)
        conn.commit()
        print('Table dropped, from statement: {}'.format(drop_tmp_table))
        cur.close()

        cur = conn.cursor()
        create_table_statement = """
            create table rf_agendor_cadastro_api_stage
                (
                cnpj VARCHAR ,
                matriz_filial VARCHAR,
                nome_fantasia VARCHAR,
                desc_situacao_cadastral VARCHAR,
                data_situacao_cadastral VARCHAR,
                data_inicio_atividade VARCHAR,
                cnae VARCHAR,
                nome_cnae_principal VARCHAR,
                cnae_fiscal_secundaria VARCHAR,
                logradouro VARCHAR,
                numero VARCHAR,
                complemento VARCHAR,
                bairro VARCHAR,
                cep VARCHAR,
                uf VARCHAR,
                nome_municipio VARCHAR,
                ddd_1 VARCHAR,
                telefone_1 VARCHAR,
                ddd_2 VARCHAR,
                telefone_2 VARCHAR,
                correio_eletronico VARCHAR,
                porte VARCHAR,
                razao_social VARCHAR,
                capital_social VARCHAR,
                natureza_juridica VARCHAR,
                cnpj_basico VARCHAR,
                socios_json VARCHAR
            );
            """
        cur.execute(create_table_statement)
        print('Table created from statement: {}'.format(create_table_statement))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

create_stage_table_postgres = PythonOperator(
    task_id='create_stage_table_postgres',
    dag=dag,
    python_callable=create_stage_table_postgres,
    provide_context=True

    )

def create_tmp_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        create_table_statement = """
            create table rf_agendor_cadastro_api_tmp
                (
                cnpj VARCHAR ,
                matriz_filial VARCHAR,
                nome_fantasia VARCHAR,
                desc_situacao_cadastral VARCHAR,
                data_situacao_cadastral DATE,
                data_inicio_atividade DATE,
                cnae VARCHAR,
                nome_cnae_principal VARCHAR,
                cnae_fiscal_secundaria VARCHAR,
                logradouro VARCHAR,
                numero VARCHAR,
                complemento VARCHAR,
                bairro VARCHAR,
                cep VARCHAR,
                uf VARCHAR,
                nome_municipio VARCHAR,
                ddd_1 VARCHAR,
                telefone_1 VARCHAR,
                ddd_2 VARCHAR,
                telefone_2 VARCHAR,
                correio_eletronico VARCHAR,
                porte VARCHAR,
                razao_social VARCHAR,
                capital_social VARCHAR,
                natureza_juridica VARCHAR,
                cnpj_basico VARCHAR,
                socios_json VARCHAR,
                PRIMARY KEY (cnpj)
            );
            """
        cur.execute(create_table_statement)
        print('Table created from statement: {}'.format(create_table_statement))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

create_tmp_table_postgres = PythonOperator(
    task_id='create_tmp_table_postgres',
    dag=dag,
    python_callable=create_tmp_table_postgres,
    provide_context=True
    )


def insert_tmp_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        insert_data_statement = """    
            insert into rf_agendor_cadastro_api_tmp
                select distinct cnpj,
                    matriz_filial,
                    nome_fantasia,
                    desc_situacao_cadastral,
                    cast(data_situacao_cadastral as date) as data_situacao_cadastral,
                    cast(data_inicio_atividade as date) as data_inicio_atividade,
                    cnae,
                    nome_cnae_principal,
                    cnae_fiscal_secundaria,
                    logradouro,
                    numero,
                    complemento,
                    bairro,
                    cep,
                    uf,
                    nome_municipio,
                    ddd_1,
                    telefone_1,
                    ddd_2,
                    telefone_2,
                    correio_eletronico,
                    porte,
                    razao_social,
                    capital_social,
                    natureza_juridica ,
                    cnpj_basico ,
                    socios_json 
                from rf_agendor_cadastro_api_stage
                where cnpj is not null
                and cnpj <> 'cnpj'
            ;
            """
        cur.execute(insert_data_statement)
        print('Table created from statement: {}'.format(insert_data_statement))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

insert_tmp_table_postgres = PythonOperator(
    task_id='insert_tmp_table_postgres',
    dag=dag,
    python_callable=insert_tmp_table_postgres,
    provide_context=True
    )


def rename_prod_to_bkp_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        # drop_tmp_table = """drop table rf_agendor_cadastro_api;"""
        rename_table = """ALTER TABLE rf_agendor_cadastro_api RENAME TO rf_agendor_cadastro_api_bkp;"""
        
        cur.execute(rename_table)
        conn.commit()
        print('Table dropped, from statement: {}'.format(rename_table))
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



rename_prod_to_bkp_table_postgres = PythonOperator(
    task_id='rename_prod_to_bkp_table_postgres',
    dag=dag,
    python_callable=rename_prod_to_bkp_table_postgres,
    provide_context=True
    )


def drop_bkp_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        drop_bkp_table = """drop table if exists rf_agendor_cadastro_api_bkp;"""
        
        cur.execute(drop_bkp_table)
        conn.commit()
        print('Table dropped, from statement: {}'.format(drop_bkp_table))
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


drop_bkp_table_postgres = PythonOperator(
    task_id='drop_bkp_table_postgres',
    dag=dag,
    python_callable=drop_bkp_table_postgres,
    provide_context=True
    )




def drop_stage_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        drop_tmp_table = """drop table rf_agendor_cadastro_api_stage;"""
        cur.execute(drop_tmp_table)
        conn.commit()
        print('Table dropped, from statement: {}'.format(drop_tmp_table))
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



drop_stage_table_postgres = PythonOperator(
    task_id='drop_stage_table_postgres',
    dag=dag,
    python_callable=drop_stage_table_postgres,
    provide_context=True
    )


def rename_tmp_to_prod_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        rename_table_statement = """
            ALTER TABLE rf_agendor_cadastro_api_tmp
            RENAME TO rf_agendor_cadastro_api
            ;
            """
        cur.execute(rename_table_statement)
        print('Table created from statement: {}'.format(rename_table_statement))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

rename_tmp_to_prod_table_postgres = PythonOperator(
    task_id='rename_tmp_to_prod_table_postgres',
    dag=dag,
    python_callable=rename_tmp_to_prod_table_postgres,
    provide_context=True
    )



def create_index_tmp_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        rename_table_statement = """
            CREATE INDEX cnpj_idx ON rf_agendor_cadastro_api_tmp (cnpj);
            ;
            """
        cur.execute(rename_table_statement)
        print('Index created for the tmp table using the statement: {}'.format(rename_table_statement))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

create_index_tmp_table_postgres = PythonOperator(
    task_id='create_index_tmp_table_postgres',
    dag=dag,
    python_callable=create_index_tmp_table_postgres,
    provide_context=True
    )






def validation_no_records_postgres_bq():
    from google.cloud import bigquery
    import pandas as pd
    import psycopg2

    # Wait 30 minutes each retry to see if the file has been loaded on the postgres stage table
    import time
    time.sleep(1800)


    bqclient = bigquery.Client()

    # Download query results.
    query_bq = """
        select count(1) as qt
        from `rf-agendor-335020.rf.rf_agendor_cadastro_api`
    """
    df_bq = (
        bqclient.query(query_bq)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    qt_bq = df_bq.qt[0]  
    print(qt_bq)

    conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

    query_postgres = """
        select count(1) as qt
        from rf_agendor_cadastro_api_stage
    """

    df_postgres = pd.read_sql_query(query_postgres, conn)
    qt_postgres = df_postgres.qt[0]
    print(qt_postgres)

    if qt_postgres >= qt_bq:
        print('The number of records on postgres table {postgres} is greater than on bigquery table {bq}'.format(bq=qt_bq, postgres=qt_postgres))
    else:
        raise Exception("The number of records on postgres table is less than on big query table, bq={bq} and postgres={postgres} ".format(bq=qt_bq, postgres=qt_postgres))

    return 'Table loaded Successfully!'


validation_no_records_postgres_bq = PythonOperator(
    task_id='validation_no_records_postgres_bq',
    dag=dag,
    python_callable=validation_no_records_postgres_bq,
    provide_context=True
    )


def validation_final_no_records_postgres_bq():
    from google.cloud import bigquery
    import pandas as pd
    import psycopg2

    bqclient = bigquery.Client()

    # Download query results.
    query_bq = """
        select count(1) as qt
        from `rf-agendor-335020.rf.rf_agendor_cadastro_api`
    """
    df_bq = (
        bqclient.query(query_bq)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    qt_bq = df_bq.qt[0]  
    print(qt_bq)

    conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS)

    query_postgres = """
        select count(1) as qt
        from rf_agendor_cadastro_api_tmp
    """

    df_postgres = pd.read_sql_query(query_postgres, conn)
    qt_postgres = df_postgres.qt[0]
    print(qt_postgres)

    if qt_postgres == qt_bq:
        print('The number of records on postgres table {postgres} is equal to bigquery table {bq}'.format(bq=qt_bq, postgres=qt_postgres))
    else:
        raise Exception("The number of records on postgres table is different than on bigquery table, bq={bq} and postgres={postgres} ".format(bq=qt_bq, postgres=qt_postgres))

    return 'Final table loaded Successfully!'


validation_final_no_records_postgres_bq = PythonOperator(
    task_id='validation_final_no_records_postgres_bq',
    dag=dag,
    python_callable=validation_final_no_records_postgres_bq,
    provide_context=True
    )


def grant_access_to_prod_table():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    
    # Credentials
    # import os
    # DB_HOST = os.environ.get('DB_HOST')
    # DB_USER = os.environ.get('DB_USER')
    # DB_PASS = os.environ.get('DB_PASS')

    print(DB_HOST)
    print(DB_USER)
    print(DB_PASS)

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        grant_access = """
            GRANT ALL PRIVILEGES ON public TO "agendor-dev";
            """

        cur.execute(grant_access)
        print('Access granted as the code: {}'.format(grant_access))
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


grant_access_to_prod_table = PythonOperator(
    task_id='grant_access_to_prod_table',
    dag=dag,
    python_callable=grant_access_to_prod_table,
    provide_context=True
    )

    
ct_qualificacoes_socios_query = """
    create external table if not exists `rf-agendor-335020.rf.qualificacoes_socios`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.QUALSCSV']
    );
    """

ct_paises_query = """
    create external table if not exists `rf-agendor-335020.rf.paises`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.PAISCSV']
    );
"""

ct_natureza_juridica_query = """
    create external table if not exists `rf-agendor-335020.rf.natureza_juridica`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.NATJUCSV']
    );
    """

ct_municipios_query = """
    create external table if not exists `rf-agendor-335020.rf.municipios`
    (
    codigo string,
    descricao string
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.MUNICCSV']
    );
"""

ct_empresas_query = """
    create external table if not exists `rf-agendor-335020.rf.empresas`
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
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.EMPRECSV']
    );
    """

ct_cnae_query = """
    create external table if not exists `rf-agendor-335020.rf.cnae`
    (
    codigo	STRING,
    descricao	STRING
    )
    OPTIONS (
    format = 'CSV',
    field_delimiter = ';',
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.CNAECSV']
    );
    """

ct_estabelecimentos_query = """
    create external table if not exists `rf-agendor-335020.rf.estabelecimentos`
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
    uris = ['gs://cnpj_rf_agendor/unzip_files_treated/*.ESTABELE']
    );
    """

ct_socios_query = """
    create external table if not exists `rf-agendor-335020.rf.socios`
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
    uris = ['gs://cnpj_rf_agendor/unzip_files/*.SOCIOCSV']
    );
    """

ct_socios_agg_json_query = """
    create or replace table 	`rf-agendor-335020.rf.socios_agg_json`
    (
    cnpj_basico STRING,
    socios_json ARRAY<STRING>
    );
    """

ct_rf_agendor_cadastro_api_query = """
    create or replace table 	`rf-agendor-335020.rf.rf_agendor_cadastro_api`
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
    socios_json STRING
    );
    """

insert_into_socios_agg_json_query = """
    insert into `rf-agendor-335020.rf.socios_agg_json`
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
insert into `rf-agendor-335020.rf.rf_agendor_cadastro_api`
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


start_dag = DummyOperator(task_id='start_dag', dag=dag)
create_external_tables = DummyOperator(task_id='create_external_tables', dag=dag)
create_tables = DummyOperator(task_id='create_tables', dag=dag)
insert_records = DummyOperator(task_id='insert_records', dag=dag)
storage_upload_files = DummyOperator(task_id='storage_upload_files', dag=dag)


#Workflow

start_dag >> create_external_tables >> [ct_qualificacoes_socios, ct_paises, ct_natureza_juridica, ct_municipios, ct_empresas, ct_cnae, ct_estabelecimentos, ct_socios] >> insert_records
start_dag >> create_tables >> [ct_socios_agg_json, ct_rf_agendor_cadastro_api] >> insert_records

insert_records >> insert_into_socios_agg_json >> insert_into_rf_agendor_cadastro_api  >> bigquery_to_storage >> list_storage_files >> compose_file >> storage_upload_files 
storage_upload_files >> create_stage_table_postgres >> storage_to_postgres_bash_command >> validation_no_records_postgres_bq >> create_tmp_table_postgres >> insert_tmp_table_postgres >> create_index_tmp_table_postgres >> validation_final_no_records_postgres_bq >> drop_bkp_table_postgres >> rename_prod_to_bkp_table_postgres >> rename_tmp_to_prod_table_postgres >> drop_stage_table_postgres >> grant_access_to_prod_table

