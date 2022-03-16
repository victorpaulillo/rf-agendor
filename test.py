import os
from google.cloud import secretmanager

def db_pass():
    database_url = os.environ.get('DB_PASS')
    print(database_url)
    return database_url



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
    print('1')
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    # 710709955498
    # secret_detail = f"projects/710709955498/secrets/{secret_id}/versions/{version_id}"
    print('2')
    # response = client.access_secret_version(request={"name": secret_detail})
    response = client.access_secret_version(secret_detail)
    print('3')
    DB_HOST = response.payload.data.decode("UTF-8")
    print(DB_HOST)
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


    return DB_HOST


lala = get_secret(project_id='rf-agendor-335020', secret_id = 'DB_HOST', version_i = 'latest')

print(lala)

