from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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
    dag_id='dag_rollback_bkp_cadastro_api',
    description=f'DAG para fazer o rollback para a versao de backup da tabela rf_cadastro_api. Caso tenha ocorrido algum problema com o processo de atualização da api de cadastro',
    schedule_interval=None,
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
)


def drop_error_table_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        drop_bkp_table = """drop table if exists rf_agendor_cadastro_api_error;"""
        
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


drop_error_table_postgres = PythonOperator(
    task_id='drop_error_table_postgres',
    dag=dag,
    python_callable=drop_error_table_postgres,
    provide_context=True
    )


def rename_prod_to_error_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        # drop_tmp_table = """drop table rf_agendor_cadastro_api;"""
        rename_table = """ALTER TABLE rf_agendor_cadastro_api RENAME TO rf_agendor_cadastro_api_error;"""
        
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



rename_prod_to_error_postgres = PythonOperator(
    task_id='rename_prod_to_error_postgres',
    dag=dag,
    python_callable=rename_prod_to_error_postgres,
    provide_context=True
    )



def rename_rollback_bkp_to_prod_postgres():
    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        # drop_tmp_table = """drop table rf_agendor_cadastro_api;"""
        rename_table = """ALTER TABLE rf_agendor_cadastro_api_bkp RENAME TO rf_agendor_cadastro_api;"""
        
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


rename_rollback_bkp_to_prod_postgres = PythonOperator(
    task_id='rename_rollback_bkp_to_prod_postgres',
    dag=dag,
    python_callable=rename_rollback_bkp_to_prod_postgres,
    provide_context=True
    )


start_rollback = DummyOperator(task_id='start_dag', dag=dag)



start_rollback >> drop_error_table_postgres >> rename_prod_to_error_postgres >> rename_rollback_bkp_to_prod_postgres


