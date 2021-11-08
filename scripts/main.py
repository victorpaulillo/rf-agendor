from google.cloud import bigquery
import base64
import pandas as pd 
# import json
# import pyarrow

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()

    if request.args:
        cnae = request.args.get('cnae')
        uf = request.args.get('uf')
        
        if (cnae is not None and uf is not None):
            client = bigquery.Client()
            query = """ select *
                        from rf.datalytics
                        where cnae = '{cnae}'
                        and uf = '{uf}'
                        ORDER BY RAND()
                        limit 100 """.format(cnae=cnae, uf=uf)
            query_job = client.query(query)
            obj = dict()
            obj["data"] = [dict(row) for row in query_job]
            return obj

        elif (cnae is not None and uf is None):
            client = bigquery.Client()
            query = """ select *
                        from rf.datalytics
                        where cnae = '{cnae}'
                        ORDER BY RAND()
                        limit 100 """.format(cnae=cnae)
            query_job = client.query(query)
            obj = dict()
            obj["data"] = [dict(row) for row in query_job]
            return obj

        elif (cnae is None and uf is not None):
            client = bigquery.Client()
            query = """ select *
                        from rf.datalytics
                        where uf = '{uf}'
                        ORDER BY RAND()
                        limit 100 """.format(uf=uf)
            query_job = client.query(query)
            obj = dict()
            obj["data"] = [dict(row) for row in query_job]
            return obj
        
        else:
            return 'Params passed not expected'

    elif request.get_json():
        request_json = request.get_json()
        cnae = request_json['cnae']
        uf = request_json['uf']
        return cnae

    else:
        client = bigquery.Client()
        query = """ select *
                    from rf.datalytics
                    ORDER BY RAND()
                    limit 100 """
        query_job = client.query(query)

        obj = dict()
        obj["data"] = [dict(row) for row in query_job]
        
        return obj