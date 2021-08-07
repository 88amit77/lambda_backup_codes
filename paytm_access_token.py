import json, math, psycopg2, logging, requests, time
from datetime import datetime, timedelta


def db_credential(db_name, typ):
    # import requests
    # import json
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
    print(response, type(response))
    if status == True:
        return response['db_detail'][typ]
    else:
        return


db_creds = db_credential('postgres', 'db_detail_for_psycopg2')
# db_creds={"endPoint":"buymore-dev-aurora.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com","userName":"postgres","passWord":"r2DfZEyyNL2VLfg2"}

RDS_HOST = db_creds['endPoint']
NAME = db_creds['userName']
PASSWORD = db_creds['passWord']
DB_NAME = "products"
PAYTM_PORTAL_ID = 6


def authorizeOrdersApi(authtoken):
    """
    """
    try:
        URL = "https://fulfillment.paytm.com/authorize"
        querystring = {"authtoken": authtoken}
        headers = {}
        response = requests.request("GET", URL, headers=headers, params=querystring)
        if response.ok:
            pass
    except Exception as e:
        message = "Exception authorizeOrdersApi --" + str(e)
        print(message)
    finally:
        pass


def fetchAuthorizeCode(client_id, user_name, password, state):
    """
    """
    try:
        data = {}
        URL = "https://persona.paytm.com/oauth2/authorize"
        payload = {"response_type": "code", "client_id": client_id, 'username': user_name, 'password': password,
                   'notredirect': True, 'state': state}
        headers = {'content-type': "application/x-www-form-urlencoded"}
        print(payload)
        response = requests.request("POST", URL, data=payload, headers=headers)
        if response.ok:
            data = response.json()
    except Exception as e:
        message = "Exception fetchAuthorizeCode --" + str(e)
        print(message)
    finally:
        return data


def fetchPaytmAccessToken(client_id, client_secret, code, state):
    """
    """
    try:
        data = {}
        URL = "https://persona.paytm.com/oauth2/token"
        payload = {"client_id": client_id, 'client_secret': client_secret, 'code': code,
                   'grant_type': "authorization_code", 'state': state}
        headers = {'content-type': "application/x-www-form-urlencoded"}
        print(payload)
        response = requests.request("POST", URL, data=payload, headers=headers)
        if response.ok:
            data = response.json()
    except Exception as e:
        message = "Exception--" + str(e)
        print(message)
    finally:
        return data


def lambda_handler(event, context):
    """
    """
    try:
        conn_products = psycopg2.connect(database="products", host=RDS_HOST, user=NAME, password=PASSWORD)
        products_cursor = conn_products.cursor()
        products_cursor.execute(
            "select username,password,authentication_attribute_values from master_portalaccountdetails  where portal_id_id={}".format(
                PAYTM_PORTAL_ID))
        accountdetails = products_cursor.fetchone()
        authentication_attribute_dic = {}
        if len(accountdetails) != 0:
            username = accountdetails[0].strip()
            password = accountdetails[1].strip()
            authentication_attribute_values = accountdetails[2]
            if len(authentication_attribute_values) != 0:
                authentication_attribute_dic = {i['name']: i['base_url'] for i in authentication_attribute_values}
                auth_token = authentication_attribute_dic.get('auth_token', None)
                client_id = authentication_attribute_dic.get('client_id', None)
                client_secret_id = authentication_attribute_dic.get('client_secret_id', None)
                # code = authentication_attribute_dic.get('code',None)
                state = authentication_attribute_dic.get('state', None)
                authorizeCodeRes = fetchAuthorizeCode(client_id, username, password, state)

                new_code = authorizeCodeRes.get('code', None)
                new_state = authorizeCodeRes.get('state', None)
                if new_code is not None and new_state is not None:
                    access_token_data = fetchPaytmAccessToken(client_id, client_secret_id, new_code, new_state)

                    if 'access_token' in access_token_data:
                        access_token = access_token_data['access_token']
                        token_expiry_time = access_token_data['token_expiry_time']

                        authorizeOrdersApi(access_token)  # authorize Orders Api
                        print('access_token' + access_token)
                        products_cursor.execute(
                            "with token as (select ('{'||index-1||',base_url}')::text[] as path from master_portalaccountdetails,jsonb_array_elements(authentication_attribute_values) with ordinality arr(authentication_attribute_value, index) where authentication_attribute_value->>'name' = 'access_token' and account_id = 9)update master_portalaccountdetails set authentication_attribute_values = jsonb_set(authentication_attribute_values, token.path, '\"" + str(
                                access_token) + "\"', false) from token where account_id=9")
                        products_cursor.execute(
                            "with token as (select ('{'||index-1||',base_url}')::text[] as path from master_portalaccountdetails,jsonb_array_elements(authentication_attribute_values) with ordinality arr(authentication_attribute_value, index) where authentication_attribute_value->>'name' = 'access_token_expires_in' and account_id = 9)update master_portalaccountdetails set authentication_attribute_values = jsonb_set(authentication_attribute_values, token.path, '\"" + str(
                                token_expiry_time) + "\"', false) from token where account_id=9")
                        conn_products.commit()

    except Exception as e:
        print(str(e))
    finally:
        conn_products.close()
        products_cursor.close()