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

ct_rf_agendor_api_query = """
    create or replace table 	`rf-agendor.rf.rf_agendor_api`
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
    socios_json ARRAY<STRING>,
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

insert_into_rf_agendor_api_query = """
insert into `rf-agendor.rf.rf_agendor_api`
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
ct_rf_agendor_api = PythonOperator(task_id='ct_rf_agendor_api',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":ct_rf_agendor_api_query})

insert_into_socios_agg_json = PythonOperator(task_id='insert_into_socios_agg_json',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":insert_into_socios_agg_json_query})
insert_into_rf_agendor_api = PythonOperator(task_id='insert_into_rf_agendor_api',dag=dag,python_callable=bigquery_execution,op_kwargs={"query":insert_into_rf_agendor_api_query})



start_dag = DummyOperator(task_id='start_dag', dag=dag)
create_external_tables = DummyOperator(task_id='create_external_tables', dag=dag)
create_tables = DummyOperator(task_id='create_tables', dag=dag)
insert_records = DummyOperator(task_id='insert_records', dag=dag)

start_dag >> create_external_tables >> [ct_qualificacoes_socios, ct_paises, ct_natureza_juridica, ct_municipios, ct_empresas, ct_cnae, ct_estabelecimentos, ct_socios] >> insert_records
start_dag >> create_tables >> [ct_socios_agg_json] >> insert_records

insert_records >> insert_into_socios_agg_json >> insert_into_rf_agendor_api 





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

