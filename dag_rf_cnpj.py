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
    print(f"Query executed: {query}")
    


ct_qualificacoes_socios = """
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

ct_paises = """
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

ct_natureza_juridica = """
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

ct_municipios = """
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

ct_empresas = """
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

ct_cnae = """
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



ct_estabelecimentos = """
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

ct_socios = """
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





ct_qualificacoes_socios = PythonOperator(task_id='ct_qualificacoes_socios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_qualificacoes_socios})
ct_paises = PythonOperator(task_id='ct_paises',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_paises})
ct_natureza_juridica = PythonOperator(task_id='ct_natureza_juridica',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_natureza_juridica})
ct_municipios = PythonOperator(task_id='ct_municipios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_municipios})
ct_empresas = PythonOperator(task_id='ct_empresas',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_empresas})
ct_cnae = PythonOperator(task_id='ct_cnae',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_cnae})
ct_estabelecimentos = PythonOperator(task_id='ct_estabelecimentos',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_estabelecimentos})
ct_socios = PythonOperator(task_id='ct_socios',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_socios})




start_dag = DummyOperator(task_id='start_dag', dag=dag)

create_external_tables = DummyOperator(task_id='create_external_tables', dag=dag)


start_dag >> create_external_tables >> [ct_qualificacoes_socios, ct_paises, ct_natureza_juridica, ct_municipios, ct_empresas, ct_cnae, ct_estabelecimentos, ct_socios]








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

# def values_function():
#     values = [0,1,2,3,4,5]
#     return values

# push_func = PythonOperator(
#         task_id='push_func',
#         provide_context=True,
#         python_callable=values_function,
#         dag=dag)

# def group_remove_speacial_char(number, **kwargs):
#         #load the values if needed in the command you plan to execute
#         file_number = "{{ task_instance.xcom_pull(task_ids='push_func') }}"
#         return PythonOperator(task_id='remove_spec_char_{}'.format(number),
#             dag=dag,python_callable=remove_spec_char,
#             op_kwargs={"bucket_name":'cnpj_rf', "file_number":number}
#             )
        
# def test(i):
#     next = ' >> '
#     return (group_remove_speacial_char(i), next)
        
# for i in values_function():
#         push_func >> test(i) 

