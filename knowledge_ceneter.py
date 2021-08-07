import datetime
import io
import os
import sys
# from io import StringIO
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

import json
import requests


def db_credential(db_name):
    url = "http://ec2-13-234-21-229.ap-south-1.compute.amazonaws.com/db_credentials/"

    payload = json.dumps({
        "data_base_name": db_name
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===>", response)
    status = response['status']
    print("payload", payload)
    if status == True:
        return {

            'response': response
        }
    else:
        return {
            'response': response
        }


def lambda_handler(event, context):
# def lambda_handler():
    department = event.get("department", None)
    category = event.get("category", None)
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    print("cred_for_sqlalchemy_users--", cred_for_sqlalchemy_users)
    # department = "Tech"
    # category = "Orders"


    try:
        if department is None and category is None:
            message = "department name and category name required"

        else:
            engine = create_engine(cred_for_sqlalchemy_users)
            query = "select * from api_knowledgecenter where department = '" + str(
                department) + "' and category = '" + str(category) + "'"
            print("query==", query)
            d_last = pd.read_sql(query, engine)
            d_last.to_csv('/tmp/z.csv',index=False)
            d_last1 = pd.read_csv('/tmp/z.csv')
            j = d_last1.to_json(orient='records')
            data_data = j
            print("data_data==", data_data)
            message="ok"

        return {
            'statusCode': 200,
            'message': message,
            'category': json.loads(data_data)
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "some error",
            'error': {e}
        }

# print(lambda_handler())