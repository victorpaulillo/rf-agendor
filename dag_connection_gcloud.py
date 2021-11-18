
from datetime import datetime, timedelta, date
from airflow import DAG
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


dag = DAG(
    dag_id='dag_connection_gcloud',
    description=f'DAG para criar uma conexao com o CLI do gcloud',
    schedule_interval=None,
    start_date=None,
    default_args=default_args,
    catchup=False,
)



def add_gcp_connection(ds, **kwargs):
    """"Add a airflow connection for GCP"""
    new_conn = Connection(
        conn_id=get_default_google_cloud_connection_id(),
        conn_type='google_cloud_platform',
    )
    scopes = [
        "https://www.googleapis.com/auth/pubsub",
        "https://www.googleapis.com/auth/datastore",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
    conn_extra = {
        "extra__google_cloud_platform__scope": ",".join(scopes),
        "extra__google_cloud_platform__project": "rf-agendor",
        "extra__google_cloud_platform__key_path": '/var/local/google_cloud_default.json'
    }
    conn_extra_json = json.dumps(conn_extra)
    new_conn.set_extra(conn_extra_json)

    session = settings.Session()
    if not (session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()
    else:
        msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
        msg = msg.format(conn_id=new_conn.conn_id)
        print(msg)


# Task to add a connection
t1 = PythonOperator(
    dag=dag,
    task_id='add_gcp_connection_python',
    python_callable=add_gcp_connection,
    provide_context=True,
)



