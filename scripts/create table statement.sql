


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

create or replace table 	`rf-agendor.rf.socios_agg_json`
(
  cnpj_basico STRING,
  socios_json ARRAY<STRING>
);

create or replace table rf.socios_agg_json as 
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






create or replace table rf.rf_agendor as 
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
;



create or replace table rf.rf_agendor2 as
select *
from rf.rf_agendor as r -- 50.203.226
left join rf.socios_agg_json as s
on s.cnpj_basico = substr(cnpj, 0, 8)
;





