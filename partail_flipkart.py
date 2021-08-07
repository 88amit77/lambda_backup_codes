import datetime
import io
import os
import sys
# from io import StringIO
import csv
import requests
# import dropbox
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
# from django.db import connection
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

# rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
# name = "postgres"
# password = "buymore2"
# ##mod1
# db_name = "products"


def lambda_handler(event, context):
#def lambda_handler():
    print(event)
    ###sqlachemy connection
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
    print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
    ###psycopg2 connection
    cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
    print("cred_for_psycopg2--", cred_for_psycopg2)
    rds_host = cred_for_psycopg2['endPoint']
    name = cred_for_psycopg2['userName']
    password = cred_for_psycopg2['passWord']
    ##products
    db_name = "products"
    body = json.loads(event['body'])
    print("body===")
    print(body)
    file = body["files"]
    print(file)

    try:

        url = file
        #url = 'https://content.dropboxapi.com/apitl/1/Alocpv7-D7h-GxcyIfo5pmVtfZJIpEtg22-NFQkSUtdgdUt0b2jYFEyTtCMxtL4JeijF0uxhfrEbrHMQAbkVW30DjmZ90pe_yoW9kl0PCFICHi88PzwKF9AB1Mp-RAt_K8Fd3ZAnRm5-frXeikovsCh26IuAcF04rGrjbucZqkH2ggwUxRtbOzuXN4JdQnjYNljeO7fMrwhwq3WBCG47z9ot-63R8uOpOHEgfNMyXGEFINxR4LsfFdr3zlJIv1YI_sgHB8t5O9SqYkxgOc5_VWdJJsCPzFNf7B9BEvU_qfuDC52jriHK3b1yCCVjVhOFxEqw-0pMrrm3fyHsPuFjBHJrHWVd72GllNA_hNntAR_cCulkncDc6vyamanoetH-9sJIQe2nw1QXfkRywb3pgNLa'
        response = requests.get(url)
        url_content = response.content
        csv_file = open('/tmp/downloaded.csv', 'wb')

        csv_file.write(url_content)
        csv_file.close()


        # data1 = pd.read_csv('/tmp/downloaded.csv', encoding = "ISO-8859-1", engine='python')
        data1 = pd.read_csv('/tmp/downloaded.csv')
        # data1['flipkart_portal_sku'] = data1['flipkart_portal_sku'].str.replace('††', '')
        # data1['flipkart_portal_sku'] = data1['flipkart_portal_sku'].str.replace('†', ' ')
        ##mod2


        data1.to_csv('/tmp/z10.csv')
        # data1.to_csv('z2.csv', index=False)
        data4 = pd.read_csv('/tmp/z10.csv')
        print(data4)
        df = pd.DataFrame(data4)

        print("df======", df)

        # drop all rows with any NaN and NaT values
        df1 = df.dropna(axis=1)
        print("df2=======", df1)
        first_column = df1.columns[0]
        df7 = df1.drop([first_column], axis=1)
        df7.to_csv('/tmp/z15.csv', index=False)

        ###to save column name in one variable
        data5 = pd.read_csv('/tmp/z10.csv')
        d = pd.read_csv('/tmp/z15.csv', nrows=1).columns.tolist()
        print("ddddd>>>", d)
        ###full column name stored in (d1)
        d1 = d
        print("ddddd1>>>", d1)

        # now drop that unique column from main csv file(d) we will use d to remove column from sql column which is already present in user csv
        d.remove("product_id")
        print("dddd2>>>>>", d)

        engine = create_engine(cred_for_sqlalchemy_products)

        query = "select product_id from flipkart_flipkartproducts"
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/z25.csv')

        cc = list(data5['product_id'])
        dd = sql[sql['product_id'].isin(cc)]
        dd.to_csv('/tmp/z22.csv', index=False)
        ddd = pd.read_csv('/tmp/z22.csv')
        ##main logic to extract full data which is matching to user's entered product_id
        a = []
        for i, j in ddd.iterrows():
            print(j['product_id'])
            k = j['product_id']
            a.append(k)
            print("k==", k)
        print("outer==a==", a)
        # to remove square brcket
        final = str(a)[1:-1]
        print(final)
        query = "select * from flipkart_flipkartproducts where product_id in " + "(" + str(final) + ")"
        print(query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/z25.csv', index=False)

        # first_column = dd.columns[0]
        # ddd = dd.drop([first_column], axis=1)
        # ddd.to_csv('z31.csv', index=False)

        # data from sql
        z = pd.read_csv('/tmp/z25.csv')
        print("zz", z)
        z.drop(d, axis=1, inplace=True)
        print("last", z.head())
        z.to_csv("/tmp/z32.csv", index=False)

        df11 = pd.read_csv("/tmp/z15.csv")
        df12 = pd.read_csv("/tmp/z32.csv")
        df22 = pd.merge(left=df12, right=df11, left_on='product_id', right_on='product_id')
        # df22 = pd.concat([df12, df11])
        df22.to_csv('/tmp/z33.csv')
        first_column = df22.columns[0]
        df7 = df22.drop([first_column], axis=1)
        df7.to_csv('/tmp/z34.csv', index=False)
        print("ddfff777", df7)
        df10 = pd.read_csv("/tmp/z34.csv")
        # working perfectly
        iters = df10.iterrows()

        ##mod7
        engine.dispose()
        for index, row in iters:
            conn = psycopg2.connect(host=rds_host,
                                    database=db_name,
                                    user=name,
                                    password=password)
            cur = conn.cursor()
            cur.execute(
                'UPDATE "flipkart_flipkartproducts" SET "flipkart_portal_sku" = %s,"flipkart_portal_unique_id" = %s,"flipkart_listing_id" = %s,"flipkart_price_rule" = %s,"flipkart_break_even_sp" = %s,"flipkart_min_break_even_sp" = %s,"flipkart_max_break_even_sp" = %s,"flipkart_vendors_price" = %s,"flipkart_purchase_order_value" = %s,"flipkart_current_selling_price" = %s,"flipkart_upload_selling_price" = %s,"flipkart_competitor_lowest_price" = %s,"flipkart_account_id" = %s,"flipkart_all_values_external_api" = %s,"flipkart_portal_category_id" = %s WHERE "flipkart_flipkartproducts"."product_id" = %s',
                [row['flipkart_portal_sku'], row['flipkart_portal_unique_id'], row['flipkart_listing_id'],
                 row['flipkart_price_rule'],
                 row['flipkart_break_even_sp'], row['flipkart_min_break_even_sp'], row['flipkart_max_break_even_sp'],
                 row['flipkart_vendors_price'],
                 row['flipkart_purchase_order_value'], row['flipkart_current_selling_price'],
                 row['flipkart_upload_selling_price'], row['flipkart_competitor_lowest_price'],
                 row['flipkart_account_id'], row['flipkart_all_values_external_api'],
                 row['flipkart_portal_category_id'],
                 row['product_id']])
            conn.commit()
            conn.close()
        ##mod8

        return {
            'statusCode': 200,
            'Message': "File upload successful",
            'body': json.dumps('Hello from partial Import flipkart_flipkartproducts lambda!')
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "File upload error",
            'error': {e},
            'body': json.dumps('Hello from partial Import flipkart_flipkartproducts lambda!')
        }


# print(lambda_handler())