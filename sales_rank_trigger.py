import json, math, psycopg2
import boto3, sys
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
    try:
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
        ##orders
        db_name_orders = "orders"
        ##products
        db_name_products = "products"
        ##users
        db_name_users = "users"
        ##warehouse
        db_name_warehouse = "warehouse"
        lambda_client = boto3.client('lambda')
        sqs = boto3.client('sqs')
        conn_products = psycopg2.connect(host=rds_host, database=db_name_products, user=name, password=password)
        cursor_product = conn_products.cursor()
        #query = "SELECT amazon_unique_id FROM amazon_amazonproducts where product_id in ('249535','236711','236715')"
        query = "SELECT amazon_unique_id FROM amazon_amazonproducts"
        print("query--->", query)
        cursor_product.execute(query)
        result = cursor_product.fetchall()
        print("total Requested products------->", len(result))
        print("result===", type(result))
        if len(result) != 0:
            amazon_unique_id_list = [str(i[0]) for i in result]
            # print("amazon_unique_id_list===",amazon_unique_id_list)
            queue_msg_length = 2000
            # queue_msg_length = 4000
            pro_list = [amazon_unique_id_list[i:i + queue_msg_length] for i in
                        range(0, len(amazon_unique_id_list), queue_msg_length)]
            delayMsg = 0
            counterVal = 0
            print("Number of items in the list = ", len(pro_list))
            # pro_list1=["169010"]
            # print("pro_list===",pro_list1)
            # for msg in pro_list1:
            # pro_list1=["169010"]
            # print("pro_list===",buymore_pro_list1)
            for msg in pro_list:
                print("msg====", msg)

                queue_url = "https://sqs.ap-south-1.amazonaws.com/868471381181/sales_rank"
                queue_message = {"asin_list": msg}
                print("queue_message===", queue_message)
                final_queue_message = json.dumps(queue_message)
                print("final_queue_message==>", final_queue_message)
                response = sqs.send_message(QueueUrl=queue_url, MessageBody=final_queue_message)

    except Exception as e:
        print(str(e))
    finally:
        conn_products.close()
        cursor_product.close()
        return {'statusCode': 200}