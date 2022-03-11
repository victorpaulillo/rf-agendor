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
    secret_id = kwargs.get('secret_id')
    version_id = kwargs.get('version_id')

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret.
    # name = client.secret_path(project_id, secret_id)

    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    # response = client.access_secret_version(request={"name": secret_detail})
    response = client.access_secret_version(secret_detail)
    DB_HOST = response.payload.data.decode("UTF-8")

    # Get the secret.
    # response = client.get_secret(name)
    
    # import ast
    # credentials = ast.literal_eval(response.LabelsEntry.value)

    # DB_HOST=credentials["data"]["DB_HOST"]
    # DB_USER=credentials["data"]["DB_USER"]
    # DB_PASS=credentials["data"]["DB_PASS"]
    # DB_HOST = response.LabelsEntry.value
    DB_USER = 'postgres'
    DB_PASS = 'SDjk127Dfg'

    create_time = response.create_time
    labels = response.labels
    key = response.LabelsEntry.key
    value = response.LabelsEntry.value


    """ Connect to the PostgreSQL database server """
    import psycopg2
    conn = None

    try:
        conn = psycopg2.connect(host=DB_HOST, database="rf", user=DB_USER, password=DB_PASS, port= '5432')

        cur = conn.cursor()
        grant_access = """
            GRANT ALL PRIVILEGES ON public TO "test";
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


    # Print data about the secret.
    print("Got secret {} with replication policy {}, {}, {}, {}, {}".format(response.name, response, create_time, labels, key, value))


test_secret_manager = PythonOperator(
    task_id='test_secret_manager',
    dag=dag,
    python_callable=get_secret,
    # op_kwargs={"project_id":'rf-agendor-335020', "secret_id":'postgres_prod', "version_id":'latest'}
    op_kwargs={"project_id":'rf-agendor-335020', "secret_id":'DB_HOST', "version_id":'latest'}
    )






    