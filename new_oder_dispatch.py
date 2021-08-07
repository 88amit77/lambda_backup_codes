import json
import os
# import dropbox
import csv
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import datetime as dt
import datetime
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
    print(event)
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

    body = json.loads(event['body'])
    print("body===")
    print(body)
    file = body["files"]
    print(file)

    try:

        #url = 'https://content.dropboxapi.com/apitl/1/AmeP49c5nmoEx__4xsjU7Yr0wC6NQFH4cJtE2krigoyqwkZiFTbvaTIRUoVrselLIaJ4kvM47rlmy3g72pfNE6c81vW6PZgWHkOm3peLXN-xPfurcPHck9c_fW42lXNoQZL2Om0Lw3HYR0z9lLvheyZIfQYNpF1wPwzH4WlWf-AC2DfIsA0S_ZMKR1xLbNQ1C0ZwqhXIfT0-_efzwE9ZIChhyHa5hmpuSJ-Esg3YDhDYi59oqHt-MbipohkFAZcCuooAKPezD_dqEg6tHyPCMikKL5kRDq_LWR1FF8tyYbTISInqxDDz9GgSFmf3KS_MojANq-dPFkI6x1BNFySXgHD8CVuytppGmpj7Q8vGTTQYoMA1QKi8-qguTTiwAuDmjV0'
        # response = requests.get(url)

        # with open('/tmp/out.csv', 'w') as f:
        #     writer = csv.writer(f)
        #     for line in response.iter_lines():
        #         writer.writerow(line.decode('utf-8').split(','))
        url = file
        #url = 'https://content.dropboxapi.com/apitl/1/AnnhgzZipW_fBMPmb7DfpKo_VQ4cfWOJvoQ5n5j21sFCWlIrGJdWtor6pWx8O0XfjacuA7oM8k-4FI9a43lqdIwtbcmKf48uo75mDhOpLvfOTHZVm12eFjp_WGF_ZpjGW8bMZzQdlcXNXYhOEYXrUH6qNmd_3gCEkR_d4PSA-g53nOUqhvO-kmVeNSuykJ0e3sdo9ug7udzxv1_2rvGXxZNa4xjw_BMFHNM_psuZue-A4RYOp_GevJToPbUb0G4dQh63kt_dMrwSMT0TAC3ZPigtKLr97Y4EL2bu6HQjJh8Pzkv3fiK_A3quq0hLKRgduzRMfTLkPHXATjXEToT2rADecZg7LaiEeu1GB-8qW80FSs94owZ0XtAUP8JB47wdTMKbrsDIws_ljwsA9xBMIZe_'
        #url = "https://content.dropboxapi.com/apitl/1/Avf7Itkplnv6L9gOGbnzmnOeBkiEPC8uJNh22i8weyMRkceKRtlXBbfIT_AOXAcinE9xpSN_4W2mkJJ4otGzFdxo2k1M_5rKABKtY-wfDLceeZXPzmQxZeNH1Zj513cebNJlauTSK1_MHVWZqJI6fts3GYUkazSHDSZ25LxpY3HN-oXeKLM0ndGIQLXYc9P2H0AcxoygS4UOqzUelRSJHJWvr2BVyo0UQJnQJ7pueOcGoE7NkmVDp97hJnivunxda1_O1v1uTRLdEIlfPcgP5KicpjO6AxRHyz6HKg7JygTkZKTznnuBI3me3zx3LaEOcuE-EmLyj309RVK0Wz43rrPRviDRUOSzPEy3jwZGz-DKQhJR_NtEEi81O2KoUZcdEpTe5Potjxm4XxzFqTAiTQ1Y"
        #url = "https://content.dropboxapi.com/apitl/1/AvdykOwobXW94n3MoQSZ0Xwxwckn-LTkFf0D1gCTOMCkiuQOVRIOv9EZdsrcuKHG4dDj_afGkUSzDrqsXZphRgjHPo4eASzb5Oo5uehFFO3-Jb5jKF9v029R3yFkH-YxViii0OT8CLjv7vvpYlJs8TfALURHjIgg1X1NbxOz0KlESh_zFN6Co8cG-8Svvgk0xe53Aa6vPnWW0mA7fWlfaaJj--MmXCCwK6u-60a5v_p7ucVQyOpkIqJhtOm2BK8P7wSzhw7bTqZ_44eEnpqMkljadbfT9nxBC8JkjkEhU-cGSzauxkY6p3ZJ78n9TXVJ3xoyJ0PGXrhYC6sscnNLunVELNy6MxPQl61u5mDwob9EjlnYbJpo6yLvL-F5gfz_kGnyP6EZUFBiivl9Jau-dgvk"
        response = requests.get(url)
        url_content = response.content
        csv_file = open('/tmp/out.csv', 'wb')

        csv_file.write(url_content)
        csv_file.close()
        # data1 = pd.read_csv('/tmp/out.csv')
        # data1 = pd.read_csv('z50.csv')
        data1 = pd.read_csv('/tmp/out.csv',
                            usecols=['order_id', 'order_item_id', 'order_date', 'dispatch_by_date', 'portal_id',
                                     'portal_sku', 'qty', 'selling_price', 'warehouse_id', 'portal_account_id'],
                            low_memory=False)
        # data1 = pd.read_csv('/tmp/out.csv',
        #                     usecols=['order_id', 'order_item_id', 'order_date', 'dispatch_by_date', 'portal_id',
        #                              'portal_sku', 'qty', 'selling_price', 'warehouse_id', 'portal_account_id'],
        #                     encoding='latin1')
        print(data1)
        data1 = data1.dropna(subset=['order_item_id'])
        data1['order_item_id'] = data1['order_item_id'].str.replace("'", '')
        data1.to_csv('/tmp/z41.csv', index=False)
        data1['product_id'] = 0
        data1['buymore_sku'] = 'None'
        data1['vendor_id'] = 0
        data1['region'] = 'None'
        data1['payment_method'] = 'None'
        data1['mrp'] = 0
        data1['tax_rate'] = 0
        data1['portal_sku'] = data1['portal_sku'].str.replace("'", " ")
        data1['portal_sku'] = data1['portal_sku'].str.replace('†', '')
        print("2nd",data1)
        # data1.to_csv('z42.csv', index=False)
        data1.to_csv('/tmp/z42.csv', index=False)
        headers = data1.head(0)
        print("headers", headers)
        headers.to_csv('/tmp/z101.csv', index=False)

        ###seprate data based on portal id
        ###amazon
        data_amazon_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_check = data_amazon_portal_check.loc[data_amazon_portal_check.portal_id == 1]

        if data_amazon_check.empty:
            pass
        else:
            data_amazon_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon = data_amazon_portal_id.loc[data_amazon_portal_id.portal_id == 1]
            data_amazon.rename(columns={'portal_sku': 'amazon_portal_sku'}, inplace=True)
            data_amazon.to_csv('/tmp/z1181.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z1181.csv')
            ddd2 = pd.read_csv('/tmp/z1181.csv', usecols=['amazon_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z1183.csv')
            ddd2['amazon_portal_sku'] = ddd2['amazon_portal_sku'].str.replace('?', ' ')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_portal_sku'])
                k = j['amazon_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,amazon_portal_sku from amazon_amazonproducts where amazon_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z1184.csv')
            ###to fetach buymore sku related to that product id
            ddd4 = pd.read_csv('/tmp/z1184.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd4.to_csv('/tmp/z1185.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd4.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k22==", k)
            print("outer==a22==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql4 = pd.read_sql(query, engine)
                sql4.to_csv('/tmp/z1186.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z1184.csv")
                df122 = pd.read_csv("/tmp/z1186.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z1187.csv', index=False)
                df232 = pd.read_csv('/tmp/z1187.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z1188.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z1191.csv')
                df332 = pd.read_csv("/tmp/z1188.csv")
                df442 = pd.read_csv("/tmp/z1191.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z1192.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z1181.csv")
                df772 = pd.read_csv("/tmp/z1192.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_portal_sku', right_on='amazon_portal_sku')
                df882.to_csv('/tmp/z1193.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z1194.csv', index=False)
                df992 = pd.read_csv('/tmp/z1194.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z1195.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z1196.csv', index=False)
                df9992 = pd.read_csv('/tmp/z1195.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z1196.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z1196.csv")
                df9988.to_csv('/tmp/z11100.csv', index=False)
                amazon = pd.read_csv("/tmp/z11100.csv")
                amazon.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###flipkart#######
        data_flipkart_portal_check = pd.read_csv('/tmp/z42.csv')
        data_flipkart_check = data_flipkart_portal_check.loc[data_flipkart_portal_check.portal_id == 2]

        if data_flipkart_check.empty:
            pass
        else:
            data_flipkart_portal_id = pd.read_csv('/tmp/z42.csv')
            data_flipkart = data_flipkart_portal_id.loc[data_flipkart_portal_id.portal_id == 2]
            data_flipkart.rename(columns={'portal_sku': 'flipkart_portal_sku'}, inplace=True)
            data_flipkart.to_csv('/tmp/z81.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z81.csv')
            ddd2 = pd.read_csv('/tmp/z81.csv', usecols=['flipkart_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z83.csv')
            ddd2['flipkart_portal_sku'] = ddd2['flipkart_portal_sku'].str.replace('?', ' ')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['flipkart_portal_sku'])
                k = j['flipkart_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,flipkart_portal_sku from flipkart_flipkartproducts where flipkart_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z84.csv')
            ###to fetach buymore sku related to that product id
            ddd4 = pd.read_csv('/tmp/z84.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd4.to_csv('/tmp/z85.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd4.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k22==", k)
            print("outer==a22==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql4 = pd.read_sql(query, engine)
                sql4.to_csv('/tmp/z86.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z84.csv")
                df122 = pd.read_csv("/tmp/z86.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z87.csv', index=False)
                df232 = pd.read_csv('/tmp/z87.csv',
                                    usecols=['product_id', 'buymore_sku', 'flipkart_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z88.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z91.csv')
                df332 = pd.read_csv("/tmp/z88.csv")
                df442 = pd.read_csv("/tmp/z91.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z92.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z81.csv")
                df772 = pd.read_csv("/tmp/z92.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='flipkart_portal_sku', right_on='flipkart_portal_sku')
                df882.to_csv('/tmp/z93.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['flipkart_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z94.csv', index=False)
                df992 = pd.read_csv('/tmp/z94.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z95.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['flipkart_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['flipkart_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'flipkart_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z96.csv', index=False)
                df9992 = pd.read_csv('/tmp/z95.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z96.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z96.csv")
                df9988.to_csv('/tmp/z100.csv', index=False)
                flipkart = pd.read_csv("/tmp/z100.csv")
                flipkart.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###paytm#######
        data_paytm_portal_check = pd.read_csv('/tmp/z42.csv')
        data_paytm_check = data_paytm_portal_check.loc[data_paytm_portal_check.portal_id == 6]

        if data_paytm_check.empty:
            pass
        else:
            data_paytm_portal_id = pd.read_csv('/tmp/z42.csv')
            data_paytm = data_paytm_portal_id.loc[data_paytm_portal_id.portal_id == 6]
            data_paytm.rename(columns={'portal_sku': 'paytm_portal_sku'}, inplace=True)
            data_paytm.to_csv('/tmp/z111.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z111.csv')
            ddd2 = pd.read_csv('/tmp/z111.csv', usecols=['paytm_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z113.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['paytm_portal_sku'])
                k = j['paytm_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,paytm_portal_sku from paytm_paytmproducts where paytm_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z114.csv')
            ###to fetach buymore sku related to that product id
            ddd4 = pd.read_csv('/tmp/z114.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd4.to_csv('/tmp/z115.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd4.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k22==", k)
            print("outer==a22==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql4 = pd.read_sql(query, engine)
                sql4.to_csv('/tmp/z116.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z114.csv")
                df122 = pd.read_csv("/tmp/z116.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z117.csv', index=False)
                df232 = pd.read_csv('/tmp/z117.csv',
                                    usecols=['product_id', 'buymore_sku', 'paytm_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z118.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z121.csv')
                df332 = pd.read_csv("/tmp/z118.csv")
                df442 = pd.read_csv("/tmp/z121.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z122.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z111.csv")

                df772 = pd.read_csv("/tmp/z122.csv")
                # df662.to_csv('z70000.csv', index=False)
                # df772.to_csv('z70001.csv', index=False)
                df882 = pd.merge(left=df772, right=df662, left_on='paytm_portal_sku', right_on='paytm_portal_sku')
                df882.to_csv('/tmp/z123.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['paytm_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z124.csv', index=False)
                df992 = pd.read_csv('/tmp/z124.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z125.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['paytm_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['paytm_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'paytm_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z126.csv', index=False)
                df9992 = pd.read_csv('/tmp/z125.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z126.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z126.csv")
                df9988.to_csv('/tmp/z110.csv', index=False)
                paytm = pd.read_csv("/tmp/z110.csv")
                # paytm.to_csv('z50000')
                paytm.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###snapdeal#######
        data_snapdealp_snapdealpproducts_portal_check = pd.read_csv('/tmp/z42.csv')
        data_snapdealp_snapdealpproducts_check = data_snapdealp_snapdealpproducts_portal_check.loc[data_snapdealp_snapdealpproducts_portal_check.portal_id == 8]

        if data_snapdealp_snapdealpproducts_check.empty:
            pass
        else:
            data_snapdealp_snapdealpproducts_portal_id = pd.read_csv('/tmp/z42.csv')
            data_snapdealp_snapdealpproducts = data_snapdealp_snapdealpproducts_portal_id.loc[data_snapdealp_snapdealpproducts_portal_id.portal_id == 8]
            data_snapdealp_snapdealpproducts.rename(columns={'portal_sku': 'snapdealp_portal_sku'}, inplace=True)
            data_snapdealp_snapdealpproducts.to_csv('/tmp/z1111.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z1111.csv')
            ddd2 = pd.read_csv('/tmp/z1111.csv', usecols=['snapdealp_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z1113.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['snapdealp_portal_sku'])
                k = j['snapdealp_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,snapdealp_portal_sku from snapdealp_snapdealpproducts where snapdealp_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z1114.csv')
            ###to fetach buymore sku related to that product id
            ddd4 = pd.read_csv('/tmp/z1114.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd4.to_csv('/tmp/z1115.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd4.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k22==", k)
            print("outer==a22==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql4 = pd.read_sql(query, engine)
                sql4.to_csv('/tmp/z1116.csv')
                # sql4.to_csv('z10000.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z1114.csv")
                df122 = pd.read_csv("/tmp/z1116.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z1117.csv', index=False)
                df232 = pd.read_csv('/tmp/z1117.csv',
                                    usecols=['product_id', 'buymore_sku', 'snapdealp_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z1118.csv', index=False)
                # df232.to_csv('z10001.csv')

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z1121.csv')
                df332 = pd.read_csv("/tmp/z1118.csv")
                df442 = pd.read_csv("/tmp/z1121.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z1122.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z1111.csv")

                df772 = pd.read_csv("/tmp/z1122.csv")
                # df662.to_csv('z80000.csv', index=False)
                # df772.to_csv('z80001.csv', index=False)
                df882 = pd.merge(left=df772, right=df662, left_on='snapdealp_portal_sku', right_on='snapdealp_portal_sku')
                df882.to_csv('/tmp/z1123.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['snapdealp_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z1124.csv', index=False)
                df992 = pd.read_csv('/tmp/z1124.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z1125.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['snapdealp_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['snapdealp_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'snapdealp_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z1126.csv', index=False)
                df9992 = pd.read_csv('/tmp/z1125.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z1126.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z1126.csv")
                df9988.to_csv('/tmp/z1110.csv', index=False)
                snapdealp = pd.read_csv("/tmp/z1110.csv")
                # snapdealp.to_csv('z500001')
                snapdealp.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###myntra
        data_myntra_portal_check = pd.read_csv('/tmp/z42.csv')
        data_myntra_check = data_myntra_portal_check.loc[data_myntra_portal_check.portal_id == 10]

        if data_myntra_check.empty:
            pass
        else:
            data_myntra_portal_id = pd.read_csv('/tmp/z42.csv')
            data_myntra = data_myntra_portal_id.loc[data_myntra_portal_id.portal_id == 10]
            data_myntra.rename(columns={'portal_sku': 'myntra_portal_sku'}, inplace=True)
            data_myntra.to_csv('/tmp/z141.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z141.csv')
            ddd2 = pd.read_csv('/tmp/z141.csv', usecols=['myntra_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z142.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['myntra_portal_sku'])
                k = j['myntra_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,myntra_portal_sku from myntra_myntraproducts where myntra_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z144.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('z144.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z145.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z146.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z144.csv")
                df122 = pd.read_csv("/tmp/z146.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z147.csv', index=False)
                df232 = pd.read_csv('/tmp/z147.csv',
                                    usecols=['product_id', 'buymore_sku', 'myntra_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z148.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z149.csv')
                df332 = pd.read_csv("/tmp/z148.csv")
                df442 = pd.read_csv("/tmp/z149.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z150.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z141.csv")
                df772 = pd.read_csv("/tmp/z150.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='myntra_portal_sku', right_on='myntra_portal_sku')
                df882.to_csv('/tmp/z153.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['myntra_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z154.csv', index=False)
                df992 = pd.read_csv('/tmp/z154.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z155.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['myntra_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['myntra_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'myntra_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z146.csv', index=False)
                df9992 = pd.read_csv('/tmp/z145.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z146.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z146.csv")
                df9988.to_csv('/tmp/z140.csv', index=False)
                myntra = pd.read_csv("/tmp/z140.csv")
                myntra.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###ajio###
        data_ajio_portal_check = pd.read_csv('/tmp/z42.csv')
        data_ajio_check = data_ajio_portal_check.loc[data_ajio_portal_check.portal_id == 12]

        if data_ajio_check.empty:
            pass
        else:
            data_ajio_portal_id = pd.read_csv('/tmp/z42.csv')
            data_ajio = data_ajio_portal_id.loc[data_ajio_portal_id.portal_id == 12]
            data_ajio.rename(columns={'portal_sku': 'ajio_portal_sku'}, inplace=True)
            data_ajio.to_csv('/tmp/z161.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z161.csv')
            ddd2 = pd.read_csv('/tmp/z161.csv', usecols=['ajio_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z162.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['ajio_portal_sku'])
                k = j['ajio_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,ajio_portal_sku from ajio_ajioproducts where ajio_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z164.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/z164.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z165.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z166.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z164.csv")
                df122 = pd.read_csv("/tmp/z166.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z167.csv', index=False)
                df232 = pd.read_csv('/tmp/z167.csv',
                                    usecols=['product_id', 'buymore_sku', 'ajio_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z168.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z169.csv')
                df332 = pd.read_csv("/tmp/z168.csv")
                df442 = pd.read_csv("/tmp/z169.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z170.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z161.csv")
                df772 = pd.read_csv("/tmp/z170.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='ajio_portal_sku',
                                 right_on='ajio_portal_sku')
                df882.to_csv('/tmp/z173.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['ajio_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z164.csv', index=False)
                df992 = pd.read_csv('/tmp/z164.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z175.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['ajio_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['ajio_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'ajio_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z166.csv', index=False)
                df9992 = pd.read_csv('/tmp/z165.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z166.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z166.csv")
                df9988.to_csv('/tmp/z171.csv', index=False)
                ajio = pd.read_csv("/tmp/z171.csv")
                ajio.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###shopclues_shopcluesproducts
        data_shopclues_portal_check = pd.read_csv('/tmp/z42.csv')
        data_shopclues_check = data_shopclues_portal_check.loc[data_shopclues_portal_check.portal_id == 13]

        if data_shopclues_check.empty:
            pass
        else:
            data_shopclues_portal_id = pd.read_csv('/tmp/z42.csv')
            data_shopclues = data_shopclues_portal_id.loc[data_shopclues_portal_id.portal_id == 13]
            data_shopclues.rename(columns={'portal_sku': 'shopclues_portal_sku'}, inplace=True)
            data_shopclues.to_csv('/tmp/z171.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z171.csv')
            ddd2 = pd.read_csv('/tmp/z171.csv', usecols=['shopclues_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z172.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['shopclues_portal_sku'])
                k = j['shopclues_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,shopclues_portal_sku from shopclues_shopcluesproducts where shopclues_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z174.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/z174.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z185.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z186.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z174.csv")
                df122 = pd.read_csv("/tmp/z186.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z187.csv', index=False)
                df232 = pd.read_csv('/tmp/z187.csv',
                                    usecols=['product_id', 'buymore_sku', 'shopclues_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z188.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z189.csv')
                df332 = pd.read_csv("/tmp/z188.csv")
                df442 = pd.read_csv("/tmp/z189.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z190.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z171.csv")
                df772 = pd.read_csv("/tmp/z190.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='shopclues_portal_sku',
                                 right_on='shopclues_portal_sku')
                df882.to_csv('/tmp/z183.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['shopclues_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z174.csv', index=False)
                df992 = pd.read_csv('/tmp/z174.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z175.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['shopclues_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['shopclues_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'shopclues_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z176.csv', index=False)
                df9992 = pd.read_csv('/tmp/z175.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z176.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z176.csv")
                df9988.to_csv('/tmp/z191.csv', index=False)
                shopclues = pd.read_csv("/tmp/z191.csv")
                shopclues.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###limeroad_limeroadproducts
        data_limeroad_portal_check = pd.read_csv('/tmp/z42.csv')
        data_limeroad_check = data_limeroad_portal_check.loc[data_limeroad_portal_check.portal_id == 14]

        if data_limeroad_check.empty:
            pass
        else:
            data_limeroad_portal_id = pd.read_csv('/tmp/z42.csv')
            data_limeroad = data_limeroad_portal_id.loc[data_limeroad_portal_id.portal_id == 14]
            data_limeroad.rename(columns={'portal_sku': 'limeroad_portal_sku'}, inplace=True)
            data_limeroad.to_csv('/tmp/z271.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z271.csv')
            ddd2 = pd.read_csv('/tmp/z271.csv', usecols=['limeroad_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z272.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['limeroad_portal_sku'])
                k = j['limeroad_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,limeroad_portal_sku from limeroad_limeroadproducts where limeroad_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z274.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/z174.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z285.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z286.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z274.csv")
                df122 = pd.read_csv("/tmp/z286.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z287.csv', index=False)
                df232 = pd.read_csv('/tmp/z287.csv',
                                    usecols=['product_id', 'buymore_sku', 'limeroad_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z288.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z289.csv')
                df332 = pd.read_csv("/tmp/z288.csv")
                df442 = pd.read_csv("/tmp/z289.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z290.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z271.csv")
                df772 = pd.read_csv("/tmp/z290.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='limeroad_portal_sku',
                                 right_on='limeroad_portal_sku')
                df882.to_csv('/tmp/z183.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['limeroad_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z274.csv', index=False)
                df992 = pd.read_csv('/tmp/z274.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z275.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['limeroad_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['limeroad_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'limeroad_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z276.csv', index=False)
                df9992 = pd.read_csv('/tmp/z275.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z276.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z276.csv")
                df9988.to_csv('/tmp/z291.csv', index=False)
                limeroad = pd.read_csv("/tmp/z291.csv")
                limeroad.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###nykaa_nykaaproducts
        data_nykaa_portal_check = pd.read_csv('/tmp/z42.csv')
        data_nykaa_check = data_nykaa_portal_check.loc[data_nykaa_portal_check.portal_id == 15]

        if data_nykaa_check.empty:
            pass
        else:
            data_nykaa_portal_id = pd.read_csv('/tmp/z42.csv')
            data_nykaa = data_nykaa_portal_id.loc[data_nykaa_portal_id.portal_id == 15]
            data_nykaa.rename(columns={'portal_sku': 'nykaa_portal_sku'}, inplace=True)
            data_nykaa.to_csv('/tmp/z371.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z371.csv')
            ddd2 = pd.read_csv('/tmp/z371.csv', usecols=['nykaa_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z372.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['nykaa_portal_sku'])
                k = j['nykaa_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,nykaa_portal_sku from nykaa_nykaaproducts where nykaa_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z374.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/z374.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z385.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z386.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z374.csv")
                df122 = pd.read_csv("/tmp/z386.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z387.csv', index=False)
                df232 = pd.read_csv('/tmp/z387.csv',
                                    usecols=['product_id', 'buymore_sku', 'nykaa_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z388.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z389.csv')
                df332 = pd.read_csv("/tmp/z388.csv")
                df442 = pd.read_csv("/tmp/z389.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z390.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z371.csv")
                df772 = pd.read_csv("/tmp/z380.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='nykaa_portal_sku',
                                 right_on='nykaa_portal_sku')
                df882.to_csv('/tmp/z383.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['nykaa_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z374.csv', index=False)
                df992 = pd.read_csv('/tmp/z374.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z375.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['nykaa_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['nykaa_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'nykaa_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z376.csv', index=False)
                df9992 = pd.read_csv('/tmp/z375.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z376.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z376.csv")
                df9988.to_csv('/tmp/z391.csv', index=False)
                nykaa = pd.read_csv("/tmp/z391.csv")
                nykaa.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###purplle_purplleproducts###
        data_purplle_portal_check = pd.read_csv('/tmp/z42.csv')
        data_purplle_check = data_purplle_portal_check.loc[data_purplle_portal_check.portal_id == 16]

        if data_purplle_check.empty:
            pass
        else:
            data_purplle_portal_id = pd.read_csv('/tmp/z42.csv')
            data_purplle = data_purplle_portal_id.loc[data_purplle_portal_id.portal_id == 16]
            data_purplle.rename(columns={'portal_sku': 'purplle_portal_sku'}, inplace=True)
            data_purplle.to_csv('/tmp/z261.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z261.csv')
            ddd2 = pd.read_csv('/tmp/z261.csv', usecols=['purplle_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z262.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['purplle_portal_sku'])
                k = j['purplle_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id,purplle_portal_sku from purplle_purplleproducts where purplle_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z264.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/z264.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z265.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z266.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z264.csv")
                df122 = pd.read_csv("/tmp/z266.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z267.csv', index=False)
                df232 = pd.read_csv('/tmp/z267.csv',
                                    usecols=['product_id', 'buymore_sku', 'purplle_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z268.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z269.csv')
                df332 = pd.read_csv("/tmp/z268.csv")
                df442 = pd.read_csv("/tmp/z269.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z270.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z261.csv")
                df772 = pd.read_csv("/tmp/z270.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='purplle_portal_sku',
                                 right_on='purplle_portal_sku')
                df882.to_csv('/tmp/z273.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['purplle_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z264.csv', index=False)
                df992 = pd.read_csv('/tmp/z264.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z275.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['purplle_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['purplle_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'purplle_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z266.csv', index=False)
                df9992 = pd.read_csv('/tmp/z265.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z266.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z266.csv")
                df9988.to_csv('/tmp/z222.csv', index=False)
                purplle = pd.read_csv("/tmp/z222.csv")
                purplle.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###onemg_onemgproducts###
        data_onemg_portal_check = pd.read_csv('/tmp/z42.csv')
        data_onemg_check = data_onemg_portal_check.loc[data_onemg_portal_check.portal_id == 17]

        if data_onemg_check.empty:
            pass
        else:
            data_onemg_portal_id = pd.read_csv('/tmp/z42.csv')
            data_onemg = data_onemg_portal_id.loc[data_onemg_portal_id.portal_id == 17]
            data_onemg.rename(columns={'portal_sku': 'onemg_portal_sku'}, inplace=True)
            data_onemg.to_csv('/tmp/z361.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z361.csv')
            ddd2 = pd.read_csv('/tmp/z361.csv', usecols=['onemg_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z362.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['onemg_portal_sku'])
                k = j['onemg_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,onemg_portal_sku from onemg_onemgproducts where onemg_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z364.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/364.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z365.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z366.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z364.csv")
                df122 = pd.read_csv("/tmp/z366.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z367.csv', index=False)
                df232 = pd.read_csv('/tmp/z367.csv',
                                    usecols=['product_id', 'buymore_sku', 'onemg_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z368.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z369.csv')
                df332 = pd.read_csv("/tmp/z368.csv")
                df442 = pd.read_csv("/tmp/z369.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z370.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z361.csv")
                df772 = pd.read_csv("/tmp/z370.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='onemg_portal_sku',
                                 right_on='onemg_portal_sku')
                df882.to_csv('/tmp/z373.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['onemg_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z364.csv', index=False)
                df992 = pd.read_csv('/tmp/364.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z375.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['onemg_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['onemg_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'onemg_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z366.csv', index=False)
                df9992 = pd.read_csv('/tmp/z365.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z366.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z366.csv")
                df9988.to_csv('/tmp/z322.csv', index=False)
                onemg = pd.read_csv("/tmp/z322.csv")
                onemg.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###tatacliq_tatacliqproducts###
        data_tatacliq_portal_check = pd.read_csv('/tmp/z42.csv')
        data_tatacliq_check = data_tatacliq_portal_check.loc[
            data_tatacliq_portal_check.portal_id == 29]

        if data_tatacliq_check.empty:
            pass
        else:
            data_tatacliq_portal_id = pd.read_csv('/tmp/z42.csv')
            data_tatacliq = data_tatacliq_portal_id.loc[
                data_tatacliq_portal_id.portal_id == 29]
            data_tatacliq.rename(columns={'portal_sku': 'tatacliq_portal_sku'}, inplace=True)
            data_tatacliq.to_csv('/tmp/z2111.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z2111.csv')
            ddd2 = pd.read_csv('/tmp/z2111.csv', usecols=['tatacliq_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z2113.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['tatacliq_portal_sku'])
                k = j['tatacliq_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,tatacliq_portal_sku from tatacliq_tatacliqproducts where tatacliq_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z2114.csv')
            ###to fetach buymore sku related to that product id
            ddd4 = pd.read_csv('/tmp/z2114.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd4.to_csv('/tmp/z2115.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd4.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k22==", k)
            print("outer==a22==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql4 = pd.read_sql(query, engine)
                sql4.to_csv('/tmp/z2116.csv')
                # sql4.to_csv('z10000.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z2114.csv")
                df122 = pd.read_csv("/tmp/z2116.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z2117.csv', index=False)
                df232 = pd.read_csv('/tmp/z2117.csv',
                                    usecols=['product_id', 'buymore_sku', 'tatacliq_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z2118.csv', index=False)
                # df232.to_csv('z10001.csv')

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z2121.csv')
                df332 = pd.read_csv("/tmp/z2118.csv")
                df442 = pd.read_csv("/tmp/z2121.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z2122.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z2111.csv")

                df772 = pd.read_csv("/tmp/z2122.csv")
                # df662.to_csv('z80000.csv', index=False)
                # df772.to_csv('z80001.csv', index=False)
                df882 = pd.merge(left=df772, right=df662, left_on='tatacliq_portal_sku',
                                 right_on='tatacliq_portal_sku')
                df882.to_csv('/tmp/z2123.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'], df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['tatacliq_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z2124.csv', index=False)
                df992 = pd.read_csv('/tmp/z2124.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id", "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id", "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z2125.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['tatacliq_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['tatacliq_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'tatacliq_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z2126.csv', index=False)
                df9992 = pd.read_csv('/tmp/z2125.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z2126.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z2126.csv")
                df9988.to_csv('/tmp/z2110.csv', index=False)
                tatacliq = pd.read_csv("/tmp/z2110.csv")
                # tatacliq.to_csv('z60000212.csv')
                tatacliq.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###first_cry_first_cryproducts###
        data_first_cry_portal_check = pd.read_csv('/tmp/z42.csv')
        data_first_cry_check = data_first_cry_portal_check.loc[data_first_cry_portal_check.portal_id == 18]

        if data_first_cry_check.empty:
            pass
        else:
            data_first_cry_portal_id = pd.read_csv('/tmp/z42.csv')
            data_first_cry = data_first_cry_portal_id.loc[data_first_cry_portal_id.portal_id == 18]
            data_first_cry.rename(columns={'portal_sku': 'first_cry_portal_sku'}, inplace=True)
            data_first_cry.to_csv('/tmp/z561.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z561.csv')
            ddd2 = pd.read_csv('/tmp/z561.csv', usecols=['first_cry_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z562.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['first_cry_portal_sku'])
                k = j['first_cry_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,first_cry_portal_sku from first_cry_first_cryproducts where first_cry_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z564.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/564.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z465.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z566.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z564.csv")
                df122 = pd.read_csv("/tmp/z566.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z567.csv', index=False)
                df232 = pd.read_csv('/tmp/z567.csv',
                                    usecols=['product_id', 'buymore_sku', 'first_cry_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z568.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z569.csv')
                df332 = pd.read_csv("/tmp/z568.csv")
                df442 = pd.read_csv("/tmp/z569.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z570.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z561.csv")
                df772 = pd.read_csv("/tmp/z570.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='first_cry_portal_sku',
                                 right_on='first_cry_portal_sku')
                df882.to_csv('/tmp/z573.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['first_cry_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z564.csv', index=False)
                df992 = pd.read_csv('/tmp/564.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z575.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['first_cry_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['first_cry_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'first_cry_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z566.csv', index=False)
                df9992 = pd.read_csv('/tmp/z565.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z566.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z566.csv")
                df9988.to_csv('/tmp/z522.csv', index=False)
                first_cry = pd.read_csv("/tmp/z522.csv")
                first_cry.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###udaan_udaanproductss###
        data_udaan_portal_check = pd.read_csv('/tmp/z42.csv')
        data_udaan_check = data_udaan_portal_check.loc[data_udaan_portal_check.portal_id == 19]

        if data_udaan_check.empty:
            pass
        else:
            data_udaan_portal_id = pd.read_csv('/tmp/z42.csv')
            data_udaan = data_udaan_portal_id.loc[data_udaan_portal_id.portal_id == 19]
            data_udaan.rename(columns={'portal_sku': 'udaan_portal_sku'}, inplace=True)
            data_udaan.to_csv('/tmp/z661.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z661.csv')
            ddd2 = pd.read_csv('/tmp/z661.csv', usecols=['udaan_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z662.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['udaan_portal_sku'])
                k = j['udaan_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,udaan_portal_sku from udaan_udaanproducts where udaan_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z664.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/664.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z665.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z666.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z664.csv")
                df122 = pd.read_csv("/tmp/z666.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z667.csv', index=False)
                df232 = pd.read_csv('/tmp/z667.csv',
                                    usecols=['product_id', 'buymore_sku', 'udaan_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z668.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z669.csv')
                df332 = pd.read_csv("/tmp/z668.csv")
                df442 = pd.read_csv("/tmp/z669.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z670.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z661.csv")
                df772 = pd.read_csv("/tmp/z670.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='udaan_portal_sku',
                                 right_on='udaan_portal_sku')
                df882.to_csv('/tmp/z673.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['udaan_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z664.csv', index=False)
                df992 = pd.read_csv('/tmp/664.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z675.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['udaan_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['udaan_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'udaan_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z666.csv', index=False)
                df9992 = pd.read_csv('/tmp/z665.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z666.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z666.csv")
                df9988.to_csv('/tmp/z622.csv', index=False)
                udaan = pd.read_csv("/tmp/z622.csv")
                udaan.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_business_amazon_businessproducts###
        data_amazon_business_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_business_check = data_amazon_business_portal_check.loc[data_amazon_business_portal_check.portal_id == 20]

        if data_amazon_business_check.empty:
            pass
        else:
            data_amazon_business_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_business = data_amazon_business_portal_id.loc[data_amazon_business_portal_id.portal_id == 20]
            data_amazon_business.rename(columns={'portal_sku': 'amazon_business_portal_sku'}, inplace=True)
            data_amazon_business.to_csv('/tmp/z761.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z761.csv')
            ddd2 = pd.read_csv('/tmp/z761.csv', usecols=['amazon_business_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z762.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_business_portal_sku'])
                k = j['amazon_business_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_business_portal_sku from amazon_business_amazon_businessproducts where amazon_business_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z764.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/764.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z765.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z766.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z764.csv")
                df122 = pd.read_csv("/tmp/z766.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z767.csv', index=False)
                df232 = pd.read_csv('/tmp/z767.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_business_portal_sku', 'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z768.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z769.csv')
                df332 = pd.read_csv("/tmp/z768.csv")
                df442 = pd.read_csv("/tmp/z769.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z770.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z761.csv")
                df772 = pd.read_csv("/tmp/z770.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_business_portal_sku',
                                 right_on='amazon_business_portal_sku')
                df882.to_csv('/tmp/z773.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_business_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z764.csv', index=False)
                df992 = pd.read_csv('/tmp/764.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z775.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_business_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_business_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_business_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z766.csv', index=False)
                df9992 = pd.read_csv('/tmp/z765.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z766.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z766.csv")
                df9988.to_csv('/tmp/z722.csv', index=False)
                amazon_business = pd.read_csv("/tmp/z722.csv")
                amazon_business.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###trade_india_trade_indiaproducts###
        data_trade_india_portal_check = pd.read_csv('/tmp/z42.csv')
        data_trade_india_check = data_trade_india_portal_check.loc[data_trade_india_portal_check.portal_id == 21]

        if data_trade_india_check.empty:
            pass
        else:
            data_trade_india_portal_id = pd.read_csv('/tmp/z42.csv')
            data_trade_india = data_trade_india_portal_id.loc[data_trade_india_portal_id.portal_id == 21]
            data_trade_india.rename(columns={'portal_sku': 'trade_india_portal_sku'}, inplace=True)
            data_trade_india.to_csv('/tmp/z861.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z861.csv')
            ddd2 = pd.read_csv('/tmp/z861.csv', usecols=['trade_india_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z862.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['trade_india_portal_sku'])
                k = j['trade_india_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,trade_india_portal_sku from trade_india_trade_indiaproducts where trade_india_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z864.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/864.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z865.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z866.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z864.csv")
                df122 = pd.read_csv("/tmp/z866.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z867.csv', index=False)
                df232 = pd.read_csv('/tmp/z867.csv',
                                    usecols=['product_id', 'buymore_sku', 'trade_india_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z868.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z869.csv')
                df332 = pd.read_csv("/tmp/z868.csv")
                df442 = pd.read_csv("/tmp/z869.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z870.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z861.csv")
                df772 = pd.read_csv("/tmp/z870.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='trade_india_portal_sku',
                                 right_on='trade_india_portal_sku')
                df882.to_csv('/tmp/z873.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['trade_india_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z864.csv', index=False)
                df992 = pd.read_csv('/tmp/864.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z875.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['trade_india_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['trade_india_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'trade_india_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z866.csv', index=False)
                df9992 = pd.read_csv('/tmp/z865.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z866.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z866.csv")
                df9988.to_csv('/tmp/z822.csv', index=False)
                trade_india = pd.read_csv("/tmp/z822.csv")
                trade_india.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_uk_amazon_ukproducts###
        data_amazon_uk_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_uk_check = data_amazon_uk_portal_check.loc[
            data_amazon_uk_portal_check.portal_id == 23]

        if data_amazon_uk_check.empty:
            pass
        else:
            data_amazon_uk_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_uk = data_amazon_uk_portal_id.loc[data_amazon_uk_portal_id.portal_id == 23]
            data_amazon_uk.rename(columns={'portal_sku': 'amazon_uk_portal_sku'}, inplace=True)
            data_amazon_uk.to_csv('/tmp/z961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z961.csv')
            ddd2 = pd.read_csv('/tmp/z961.csv', usecols=['amazon_uk_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_uk_portal_sku'])
                k = j['amazon_uk_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_uk_portal_sku from amazon_uk_amazon_ukproducts where amazon_uk_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z964.csv")
                df122 = pd.read_csv("/tmp/z966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z967.csv', index=False)
                df232 = pd.read_csv('/tmp/z967.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_uk_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z969.csv')
                df332 = pd.read_csv("/tmp/z968.csv")
                df442 = pd.read_csv("/tmp/z969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z961.csv")
                df772 = pd.read_csv("/tmp/z970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_uk_portal_sku',
                                 right_on='amazon_uk_portal_sku')
                df882.to_csv('/tmp/z973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_uk_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z964.csv', index=False)
                df992 = pd.read_csv('/tmp/964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_uk_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_uk_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_uk_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z966.csv")
                df9988.to_csv('/tmp/z922.csv', index=False)
                amazon_uk = pd.read_csv("/tmp/z922.csv")
                amazon_uk.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###indiamart_indiamartproducts###
        data_indiamart_portal_check = pd.read_csv('/tmp/z42.csv')
        data_indiamart_check = data_indiamart_portal_check.loc[
            data_indiamart_portal_check.portal_id == 22]

        if data_indiamart_check.empty:
            pass
        else:
            data_indiamart_portal_id = pd.read_csv('/tmp/z42.csv')
            data_indiamart = data_indiamart_portal_id.loc[data_indiamart_portal_id.portal_id == 22]
            data_indiamart.rename(columns={'portal_sku': 'indiamart_portal_sku'}, inplace=True)
            data_indiamart.to_csv('/tmp/z1961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z1961.csv')
            ddd2 = pd.read_csv('/tmp/z1961.csv', usecols=['indiamart_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z1962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['indiamart_portal_sku'])
                k = j['indiamart_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,indiamart_portal_sku from indiamart_indiamartproducts where indiamart_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z1964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/1964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z1965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z1966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z1964.csv")
                df122 = pd.read_csv("/tmp/z1966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z1967.csv', index=False)
                df232 = pd.read_csv('/tmp/z1967.csv',
                                    usecols=['product_id', 'buymore_sku', 'indiamart_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z1968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z1969.csv')
                df332 = pd.read_csv("/tmp/z1968.csv")
                df442 = pd.read_csv("/tmp/z1969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z1970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z1961.csv")
                df772 = pd.read_csv("/tmp/z1970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='indiamart_portal_sku',
                                 right_on='indiamart_portal_sku')
                df882.to_csv('/tmp/z1973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['indiamart_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z1964.csv', index=False)
                df992 = pd.read_csv('/tmp/1964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z1975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['indiamart_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['indiamart_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'indiamart_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z1966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z1965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z1966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z1966.csv")
                df9988.to_csv('/tmp/z1922.csv', index=False)
                indiamart = pd.read_csv("/tmp/z1922.csv")
                indiamart.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_us_amazon_usproducts###
        data_amazon_us_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_us_check = data_amazon_us_portal_check.loc[
            data_amazon_us_portal_check.portal_id == 24]

        if data_amazon_us_check.empty:
            pass
        else:
            data_amazon_us_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_us = data_amazon_us_portal_id.loc[data_amazon_us_portal_id.portal_id == 24]
            data_amazon_us.rename(columns={'portal_sku': 'amazon_us_portal_sku'}, inplace=True)
            data_amazon_us.to_csv('/tmp/z2961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z2961.csv')
            ddd2 = pd.read_csv('/tmp/z2961.csv', usecols=['amazon_us_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z2962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_us_portal_sku'])
                k = j['amazon_us_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_us_portal_sku from amazon_us_amazon_usproducts where amazon_us_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z2964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/2964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z2965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z2966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z2964.csv")
                df122 = pd.read_csv("/tmp/z2966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z2967.csv', index=False)
                df232 = pd.read_csv('/tmp/z2967.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_us_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z2968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z2969.csv')
                df332 = pd.read_csv("/tmp/z2968.csv")
                df442 = pd.read_csv("/tmp/z2969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z2970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z2961.csv")
                df772 = pd.read_csv("/tmp/z2970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_us_portal_sku',
                                 right_on='amazon_us_portal_sku')
                df882.to_csv('/tmp/z2973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_us_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z2964.csv', index=False)
                df992 = pd.read_csv('/tmp/2964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z2975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_us_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_us_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_us_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z2966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z2965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z2966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z2966.csv")
                df9988.to_csv('/tmp/z2922.csv', index=False)
                amazon_us = pd.read_csv("/tmp/z2922.csv")
                amazon_us.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###bigways_bigwaysproducts###
        data_bigways_bigways_portal_check = pd.read_csv('/tmp/z42.csv')
        data_bigways_bigways_check = data_bigways_bigways_portal_check.loc[
            data_bigways_bigways_portal_check.portal_id == 28]

        if data_bigways_bigways_check.empty:
            pass
        else:
            data_bigways_bigways_portal_id = pd.read_csv('/tmp/z42.csv')
            data_bigways_bigways = data_bigways_bigways_portal_id.loc[data_bigways_bigways_portal_id.portal_id == 28]
            data_bigways_bigways.rename(columns={'portal_sku': 'bigways_bigways_portal_sku'}, inplace=True)
            data_bigways_bigways.to_csv('/tmp/z3961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z3961.csv')
            ddd2 = pd.read_csv('/tmp/z3961.csv', usecols=['bigways_bigways_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z3962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['bigways_bigways_portal_sku'])
                k = j['bigways_bigways_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,bigways_bigways_portal_sku from bigways_bigwaysproducts where bigways_bigways_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z3964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/3964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z3965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z3966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z3964.csv")
                df122 = pd.read_csv("/tmp/z3966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z3967.csv', index=False)
                df232 = pd.read_csv('/tmp/z3967.csv',
                                    usecols=['product_id', 'buymore_sku', 'bigways_bigways_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z3968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z3969.csv')
                df332 = pd.read_csv("/tmp/z3968.csv")
                df442 = pd.read_csv("/tmp/z3969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z3970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z3961.csv")
                df772 = pd.read_csv("/tmp/z3970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='bigways_bigways_portal_sku',
                                 right_on='bigways_bigways_portal_sku')
                df882.to_csv('/tmp/z3973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['bigways_bigways_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z3964.csv', index=False)
                df992 = pd.read_csv('/tmp/3964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z3975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['bigways_bigways_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['bigways_bigways_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'bigways_bigways_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z3966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z3965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z3966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z3966.csv")
                df9988.to_csv('/tmp/z3922.csv', index=False)
                bigways_bigways = pd.read_csv("/tmp/z3922.csv")
                bigways_bigways.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_australia_amazon_australiaproducts###
        data_amazon_australia_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_australia_check = data_amazon_australia_portal_check.loc[
            data_amazon_australia_portal_check.portal_id == 25]

        if data_amazon_australia_check.empty:
            pass
        else:
            data_amazon_australia_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_australia = data_amazon_australia_portal_id.loc[
                data_amazon_australia_portal_id.portal_id == 25]
            data_amazon_australia.rename(columns={'portal_sku': 'amazon_australia_portal_sku'}, inplace=True)
            data_amazon_australia.to_csv('/tmp/z4961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z4961.csv')
            ddd2 = pd.read_csv('/tmp/z4961.csv', usecols=['amazon_australia_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z4962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_australia_portal_sku'])
                k = j['amazon_australia_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_australia_portal_sku from amazon_australia_amazon_australiaproducts where amazon_australia_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z4964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/4964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z4965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z4966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z4964.csv")
                df122 = pd.read_csv("/tmp/z4966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z4967.csv', index=False)
                df232 = pd.read_csv('/tmp/z4967.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_australia_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z4968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z4969.csv')
                df332 = pd.read_csv("/tmp/z4968.csv")
                df442 = pd.read_csv("/tmp/z4969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z4970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z4961.csv")
                df772 = pd.read_csv("/tmp/z4970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_australia_portal_sku',
                                 right_on='amazon_australia_portal_sku')
                df882.to_csv('/tmp/z4973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_australia_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z4964.csv', index=False)
                df992 = pd.read_csv('/tmp/4964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z4975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_australia_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_australia_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_australia_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z4966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z4965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z4966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z4966.csv")
                df9988.to_csv('/tmp/z4922.csv', index=False)
                amazon_australia = pd.read_csv("/tmp/z4922.csv")
                amazon_australia.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_uae_amazon_uaeproducts###
        data_amazon_uae_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_uae_check = data_amazon_uae_portal_check.loc[
            data_amazon_uae_portal_check.portal_id == 26]

        if data_amazon_uae_check.empty:
            pass
        else:
            data_amazon_uae_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_uae = data_amazon_uae_portal_id.loc[
                data_amazon_uae_portal_id.portal_id == 26]
            data_amazon_uae.rename(columns={'portal_sku': 'amazon_uae_portal_sku'}, inplace=True)
            data_amazon_uae.to_csv('/tmp/z5961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z5961.csv')
            ddd2 = pd.read_csv('/tmp/z5961.csv', usecols=['amazon_uae_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z5962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_uae_portal_sku'])
                k = j['amazon_uae_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_uae_portal_sku from amazon_uae_amazon_uaeproducts where amazon_uae_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z5964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/5964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z5965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z5966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z5964.csv")
                df122 = pd.read_csv("/tmp/z5966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z5967.csv', index=False)
                df232 = pd.read_csv('/tmp/z5967.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_uae_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z5968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z5969.csv')
                df332 = pd.read_csv("/tmp/z5968.csv")
                df442 = pd.read_csv("/tmp/z5969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z5970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z5961.csv")
                df772 = pd.read_csv("/tmp/z5970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_uae_portal_sku',
                                 right_on='amazon_uae_portal_sku')
                df882.to_csv('/tmp/z5973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_uae_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z5964.csv', index=False)
                df992 = pd.read_csv('/tmp/5964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z5975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_uae_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_uae_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_uae_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z5966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z5965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z5966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z5966.csv")
                df9988.to_csv('/tmp/z5922.csv', index=False)
                amazon_uae = pd.read_csv("/tmp/z5922.csv")
                amazon_uae.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###amazon_japan_amazon_japanproducts###
        data_amazon_japan_portal_check = pd.read_csv('/tmp/z42.csv')
        data_amazon_japan_check = data_amazon_japan_portal_check.loc[
            data_amazon_japan_portal_check.portal_id == 27]

        if data_amazon_japan_check.empty:
            pass
        else:
            data_amazon_japan_portal_id = pd.read_csv('/tmp/z42.csv')
            data_amazon_japan = data_amazon_japan_portal_id.loc[
                data_amazon_japan_portal_id.portal_id == 27]
            data_amazon_japan.rename(columns={'portal_sku': 'amazon_japan_portal_sku'}, inplace=True)
            data_amazon_japan.to_csv('/tmp/z6961.csv', index=False)
            ddd_ddd1 = pd.read_csv('/tmp/z6961.csv')
            ddd2 = pd.read_csv('/tmp/z6961.csv', usecols=['amazon_japan_portal_sku'], low_memory=False)
            print("dddd2", ddd2)
            ddd2.to_csv('/tmp/z6962.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd2.iterrows():
                print(j['amazon_japan_portal_sku'])
                k = j['amazon_japan_portal_sku']
                a.append(k)
                print("k11==", k)
            print("outer==a11==", a)
            # to remove square brcket
            final2 = str(a)[1:-1]
            print(final2)
            engine = create_engine(
                cred_for_sqlalchemy_products)
            query = "select product_id,amazon_japan_portal_sku from amazon_japan_amazon_japanproducts where amazon_japan_portal_sku in " + "(" + str(
                final2) + ")"
            sql3 = pd.read_sql(query, engine)
            sql3.to_csv('/tmp/z6964.csv')
            ###to fetach buymore sku related to that product id
            ddd5 = pd.read_csv('/tmp/6964.csv', usecols=['product_id'], low_memory=False)
            print("dddd2", ddd4)
            ddd5.to_csv('/tmp/z6965.csv')
            ##main logic to extract product_id data which is matching to user's entered portal_sku
            a = []
            for i, j in ddd5.iterrows():
                print(j['product_id'])
                k = j['product_id']
                a.append(k)
                print("k33==", k)
            print("outer==a33==", a)
            if a != []:
                # to remove square brcket
                final3 = str(a)[1:-1]
                print(final3)
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,buymore_sku,product_mrp,hsn_code_id_id from master_masterproduct where product_id in " + "(" + str(
                    final3) + ")"
                sql5 = pd.read_sql(query, engine)
                sql5.to_csv('/tmp/z6966.csv')
                ##merging two files to get buymore sku,product_id and portal_sku
                df112 = pd.read_csv("/tmp/z6964.csv")
                df122 = pd.read_csv("/tmp/z6966.csv")
                df222 = pd.merge(left=df122, right=df112, left_on='product_id', right_on='product_id')
                df222.to_csv('/tmp/z6967.csv', index=False)
                df232 = pd.read_csv('/tmp/z6967.csv',
                                    usecols=['product_id', 'buymore_sku', 'amazon_japan_portal_sku',
                                             'product_mrp',
                                             'hsn_code_id_id'],
                                    low_memory=False)
                df232.to_csv('/tmp/z6968.csv', index=False)

                ###for tax rate extraction from based on hsn_code_id
                engine = create_engine(
                    cred_for_sqlalchemy_products)
                query = "select product_id,max_rate,min_rate,threshold_amount from master_masterproduct join calculation_hsncoderate on master_masterproduct.hsn_code_id_id = calculation_hsncoderate.hsn_rate_id where product_id in " + "(" + str(
                    final3) + ")"
                ddsql = pd.read_sql(query, engine)
                ddsql.to_csv('/tmp/z6969.csv')
                df332 = pd.read_csv("/tmp/z6968.csv")
                df442 = pd.read_csv("/tmp/z6969.csv")
                df552 = pd.merge(left=df442, right=df332, left_on='product_id', right_on='product_id')
                df552.to_csv('/tmp/z6970.csv', index=False)
                ##final data after merging with initial data and calculated data
                df662 = pd.read_csv("/tmp/z6961.csv")
                df772 = pd.read_csv("/tmp/z6970.csv")
                df882 = pd.merge(left=df772, right=df662, left_on='amazon_japan_portal_sku',
                                 right_on='amazon_japan_portal_sku')
                df882.to_csv('/tmp/z6973.csv', index=False)

                df882['tax_rate'] = np.where(df882['selling_price'] < df882['threshold_amount'],
                                             df882['min_rate'],
                                             df882['max_rate'])
                df882['portal_sku'] = df882['amazon_japan_portal_sku']
                df882['mrp'] = df882['product_mrp']
                df882['product_id'] = df882['product_id_x']
                df882['buymore_sku'] = df882['buymore_sku_x']
                df882.to_csv('/tmp/z6964.csv', index=False)
                df992 = pd.read_csv('/tmp/6964.csv',
                                    usecols=["buymore_sku", "product_id", "order_id", "order_item_id",
                                             "order_date",
                                             "dispatch_by_date", "portal_id", "portal_account_id", "portal_sku",
                                             "qty", "selling_price", "mrp", "tax_rate", "warehouse_id",
                                             "region",
                                             "payment_method", "vendor_id"],
                                    low_memory=False)
                df992.to_csv('/tmp/z6975.csv', index=False)
                #####################
                # #logic to add data which have product id =0/null

                cc1 = list(df882['amazon_japan_portal_sku'])
                ddd_d2 = ddd_ddd1[~ddd_ddd1['amazon_japan_portal_sku'].isin(cc1)]

                ######need to modify for others portals using if else conditions
                ddd_d2 = ddd_d2.rename(columns={'amazon_japan_portal_sku': 'portal_sku'})
                # ddd_d.rename(columns={'amazon_portal_sku':'portal_sku'}, inplace=True)
                ddd_d2.to_csv('/tmp/z6966.csv', index=False)
                df9992 = pd.read_csv('/tmp/z6965.csv',
                                     usecols=["order_id", "order_item_id", "order_date", "dispatch_by_date",
                                              "portal_id",
                                              "portal_sku", "qty", "selling_price", "warehouse_id",
                                              "portal_account_id",
                                              "product_id", "buymore_sku", "vendor_id", "region",
                                              "payment_method",
                                              "mrp", "tax_rate"],
                                     low_memory=False)
                # for proper ordering
                df9992 = df9992[["order_id", "order_item_id", "order_date", "dispatch_by_date", "portal_id",
                                 "portal_sku", "qty", "selling_price", "warehouse_id", "portal_account_id",
                                 "product_id", "buymore_sku", "vendor_id", "region", "payment_method",
                                 "mrp", "tax_rate"]]

                ##combining both matched and unmatched csv file
                df9992.to_csv('/tmp/z6966.csv', mode='a', index=False, header=False)
                df9988 = pd.read_csv("/tmp/z6966.csv")
                df9988.to_csv('/tmp/z6922.csv', index=False)
                amazon_japan = pd.read_csv("/tmp/z6922.csv")
                amazon_japan.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)
            else:
                pass

        ###logic to combine all prtals data in one file(peivious and new )
        data1 = pd.read_csv('/tmp/z42.csv')
        final_csv = pd.read_csv('/tmp/z101.csv')
        dddd = list(final_csv['order_item_id'])
        last = data1[~data1['order_item_id'].isin(dddd)]
        last.to_csv('/tmp/z101.csv', mode='a', index=False, header=False)

        ##main logic for data  validation between two user data and db data files
        df1000 = pd.read_csv("/tmp/z101.csv")
        ###imp thing
        df1000["order_item_id"] = df1000["order_item_id"].astype(str)
        engine = create_engine(
            cred_for_sqlalchemy_orders)
        query = 'select order_item_id from api_neworder'
        sql22 = pd.read_sql(query, engine)
        cc = list(sql22['order_item_id'])
        dd = df1000[~df1000['order_item_id'].isin(cc)]
        dd.to_csv('/tmp/z1000.csv', index=False)
        dd['portal_sku'] = dd['portal_sku'].str.replace('†', '')
        # dd.to_csv('z6000001')
        # dd['order_item_id'] = dd['order_item_id'].str.replace("'", '')
        if dd.empty:
            pass
        else:
            engine5 = create_engine(
                cred_for_sqlalchemy_orders)
            dd.to_sql(
                name='api_neworder',
                con=engine5,
                index=False,
                if_exists='append'
            )
            print('data imported in new orders table')
        ##++++++++++++++++++##
        ###now for dispatch details import
        # data2 = pd.read_csv('z50.csv')
        data2 = pd.read_csv('/tmp/out.csv')
        data2 = data2.dropna(subset=['order_item_id'])
        data2['order_item_id'] = data2['order_item_id'].str.replace("'", '')
        data2.to_csv('/tmp/z58.csv', index=False)

        data2['dd_id_id'] = 0
        data2['location_latitude'] = 0
        data2['location_longitude'] = 0
        data2['bin_Id'] = ''
        data2['bin_confirm'] = ''
        data2['is_mark_placed'] = 'False'
        data2['have_invoice_file'] = 'False'
        data2['packing_status'] = 'False'
        data2['is_dispatch'] = 'False'
        data2['dispatch_confirmed'] = 'False'
        data2['mf_id_id'] = ''
        data2['is_shipment_create'] = 'False'
        data2['awb'] = 'None'
        data2['courier_partner'] = 'None'
        data2['shipment_id'] = 'None'
        data2['fulfillment_model'] = 'merchant'
        data2['is_canceled'] = 'False'
        data2['dd_cancelledpaymentstatus'] = 'False'
        data2['cancel_inward_bin'] = '0'
        data2['picklist_id'] = 0
        z = datetime.datetime.now()
        data2['created_at'] = z
        data2['update_at'] = z
        data2.to_csv('/tmp/z59.csv', index=False)

        data3 = pd.read_csv('/tmp/z59.csv',
                            usecols=["dd_id_id", "order_item_id", "name", "address", "pincode", "location_latitude",
                                     "location_longitude",
                                     "email_id", "phone",
                                     "status", "l_b_h_w", "bin_Id", "bin_confirm", "picklist_id", "is_mark_placed",
                                     "have_invoice_file", "packing_status",
                                     "is_dispatch", "dispatch_confirmed", "mf_id_id", "is_shipment_create", "awb",
                                     "courier_partner",
                                     "shipment_id", "fulfillment_model", "is_canceled", "dd_cancelledpaymentstatus",
                                     "dd_paymentstatus",
                                     "cancel_inward_bin", "created_at", "update_at"],
                            low_memory=False)
        data3.to_csv('/tmp/z60.csv', index=False)

        data3['is_mark_placed'] = np.where(data3['status'] == 'dispatched', 'true', 'true')
        data3['have_invoice_file'] = np.where(data3['status'] == 'dispatched', 'False', 'False')
        data3['packing_status'] = np.where(data3['status'] == 'dispatched', 'true', 'true')
        data3['is_dispatch'] = np.where(data3['status'] == 'dispatched', 'true', 'true')
        data3['dispatch_confirmed'] = np.where(data3['status'] == 'dispatched', 'False', 'True')

        # data3.to_csv('z61.csv', index=False)
        data3.to_csv('/tmp/z200.csv', index=False)
        data4 = pd.read_csv('/tmp/z200.csv')
        # data4 = pd.read_csv('z61.csv',
        #                     usecols=[
        #                              "status", "is_mark_placed",
        #                              "have_invoice_file", "packing_status",
        #                              "is_dispatch", "dispatch_confirmed"],
        #                     low_memory=False)

        data4['is_mark_placed'] = np.where(data4['status'] == 'created', 'False', data4['is_mark_placed'])
        data4['have_invoice_file'] = np.where(data4['status'] == 'created', 'False', data4['have_invoice_file'])
        data4['packing_status'] = np.where(data4['status'] == 'created', 'False', data4['packing_status'])
        data4['is_dispatch'] = np.where(data4['status'] == 'created', 'False', data4['is_dispatch'])
        data4['dispatch_confirmed'] = np.where(data4['status'] == 'created', 'False', data4['dispatch_confirmed'])
        data4.to_csv('/tmp/z201.csv', index=False)

        ###now have to featch dd_id using order_item_id(unique)
        a = []
        for i, j in data4.iterrows():
            print(j['order_item_id'])
            k = j['order_item_id']
            k1 = str(k)
            a.append(k1)
            print("k==", k1)
        print("outer==a==", a)
        # to remove square brcket
        final4 = str(a)[1:-1]
        # print(final)
        engine4 = create_engine(
            cred_for_sqlalchemy_orders)
        query4 = "select dd_id,order_item_id from api_neworder where order_item_id in " + "(" + str(final4) + ")"
        sql4 = pd.read_sql(query4, engine4)
        sql4.to_csv('/tmp/z61.csv')
        ###merge both file then remove excess fileds and then import it in dispatch detail table
        df99 = pd.read_csv("/tmp/z201.csv")
        df1010 = pd.read_csv("/tmp/z61.csv")
        df1111 = pd.merge(left=df99, right=df1010, left_on='order_item_id', right_on='order_item_id')
        df1111.to_csv('/tmp/z62.csv', index=False)
        df1111['dd_id_id'] = df1111['dd_id']
        df1111.to_csv('/tmp/z63.csv')
        data_last = pd.read_csv('/tmp/z63.csv',
                                usecols=["dd_id_id", "order_item_id", "name", "address", "pincode", "location_latitude",
                                         "location_longitude",
                                         "email_id", "phone",
                                         "status", "l_b_h_w", "bin_Id", "bin_confirm", "picklist_id", "is_mark_placed",
                                         "have_invoice_file", "packing_status",
                                         "is_dispatch", "dispatch_confirmed", "mf_id_id", "is_shipment_create", "awb",
                                         "courier_partner",
                                         "shipment_id", "fulfillment_model", "is_canceled", "dd_cancelledpaymentstatus",
                                         "dd_paymentstatus",
                                         "cancel_inward_bin", "created_at", "update_at"],
                                low_memory=False)
        data_last.to_csv('/tmp/z1001.csv', index=False)
        ##validation
        engine = create_engine(
            cred_for_sqlalchemy_orders)
        query7 = 'select dd_id_id from api_dispatchdetails'
        sql40 = pd.read_sql(query7, engine)
        sql40 = sql40.drop_duplicates(keep='first')
        sql40.to_csv('/tmp/z1002.csv', index=False)
        # sql444 = pd.read_csv('z3000.csv')
        ##main logic for data  validation between two user data and db data files
        cc5 = list(sql40['dd_id_id'])
        data_last1 = data_last[~data_last['dd_id_id'].isin(cc5)]
        data_last1.drop(['order_item_id'],  axis=1, inplace=True)
        data_last1.to_csv('/tmp/z1003.csv', index=False)
        # data_last1.to_csv('z6000002')
        if data_last1.empty:
            pass
        else:
            engine4 = create_engine(
                cred_for_sqlalchemy_orders)

            data_last1.to_sql(
                name='api_dispatchdetails',
                con=engine4,
                index=False,
                if_exists='append'
            )
            print('data imported in dispatch details table')
        engine.dispose()
        return {
            'statusCode': 200,
            'Message': "File upload successful",
            'body': json.dumps('Hello from Import neworder lambda!')
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "File upload error",
            'error': {e},
            'body': json.dumps('Hello from Import neworder lambda!')
        }

# print(lambda_handler())