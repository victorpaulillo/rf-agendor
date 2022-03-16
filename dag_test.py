from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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
    dag_id='dag_test',
    description=f'DAG para testar o scheduler',
    start_date=datetime(2021, 4, 7, tzinfo=local_tz),
    default_args=default_args,
    catchup=False,
    schedule_interval= '0 23 * * 3',
)



start_dag = DummyOperator(task_id='start_dag', dag=dag)


import os
def db_pass():
    database_url = os.environ.get('DB_PASS')
    database_url2 = os.environ.get('AIRFLOW_CONN_PASS')
    database_url3 = os.environ.get('AIRFLOW_CONN_HOST')
    print(database_url)
    print(database_url2)
    print(database_url3)
    return database_url, database_url2, database_url3

bigquery_to_storage = PythonOperator(
    task_id='db_pass',
    dag=dag,
    python_callable=db_pass,
    provide_context=True
    )


# def get_secret(project_id, secret_id):
def get_secret(**kwargs):
    """
    Get information about the given secret. This only returns metadata about
    the secret container, not any secret material.
    """
    project_id = kwargs.get('project_id')
    secret_id_host = kwargs.get('secret_id_host')
    secret_id_user = kwargs.get('secret_id_user')
    secret_id_pass = kwargs.get('secret_id_pass')
    version_id = kwargs.get('version_id')

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret.
    secret_detail_host = f"projects/{project_id}/secrets/{secret_id_host}/versions/{version_id}"
    response_host = client.access_secret_version(secret_detail_host)
    DB_HOST = response_host.payload.data.decode("UTF-8")
    print(DB_HOST)
    secret_detail_user = f"projects/{project_id}/secrets/{secret_id_user}/versions/{version_id}"
    response_user = client.access_secret_version(secret_detail_user)
    DB_USER = response_user.payload.data.decode("UTF-8")
    print(DB_USER)
    secret_detail_pass = f"projects/{project_id}/secrets/{secret_id_pass}/versions/{version_id}"
    response_pass = client.access_secret_version(secret_detail_pass)
    DB_PASS = response_pass.payload.data.decode("UTF-8")
    print(DB_PASS)

    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        grant_access = """
            GRANT ALL PRIVILEGES ON TABLE rf_agendor_cadastro_api TO test;
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



test_secret_manager = PythonOperator(
    task_id='test_secret_manager',
    dag=dag,
    python_callable=get_secret,
    op_kwargs={"project_id":'rf-agendor-335020', "secret_id_host":'DB_HOST', "secret_id_user":'DB_USER', "secret_id_pass":'DB_PASS',  "version_id":'latest'}
    )






    