import os
def db_pass():
    database_url = os.environ.get('DB_PASS')
    print(database_url)
    return database_url



print(db_pass())