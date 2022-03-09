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


# def access_secret_version(project_id, secret_id, version_id):
def access_secret_version(**kwargs):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    project_id = kwargs.get('project_id')
    secret_id = kwargs.get('secret_id')
    version_id = kwargs.get('version_id')

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    print("Plaintext: {}".format(payload))



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
    name = client.secret_path(project_id, secret_id)

    # Get the secret.
    response = client.get_secret(name)

    # Get the replication policy.
    if "automatic" in response.replication:
        replication = "AUTOMATIC"
    elif "user_managed" in response.replication:
        replication = "MANAGED"
    else:
        raise "Unknown replication {}".format(response.replication)

    # Print data about the secret.
    print("Got secret {} with replication policy {}".format(response.name, replication))


test_secret_manager = PythonOperator(
    task_id='test_secret_manager',
    dag=dag,
    python_callable=get_secret,
    op_kwargs={"project_id":'rf-agendor-335020', "secret_id":'test', "version_id":'latest'}
    )




    