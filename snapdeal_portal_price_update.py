import json, math, time, csv, logging
from datetime import datetime, timedelta
import requests, dropbox, psycopg2
from pymongo import MongoClient

SNAPDEAL_PORTAL_ID = 8
access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'


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
    """
    """
    try:
        total_success = total_fail = 0

        conn_products = psycopg2.connect(host=rds_host,
                                         database=db_name_products,
                                         user=name,
                                         password=password)

        ## FOR Mongodb Request
        client = MongoClient(
            'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
        db = client.wms
        price_update_table = db.portal_price_update_logs

        file_name = "{0}.csv".format(datetime.now().strftime("%Y%m%d%H%M%S"))
        file_path = '/tmp/' + file_name
        file_obj = open(file_path, "w+")
        fieldnamesList = ['product_id', 'snapdeal_portal_sku', 'Res']
        writer = csv.DictWriter(file_obj, fieldnames=fieldnamesList)
        writer.writeheader()

        products_cursor = conn_products.cursor()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        statusCode = 200
        message = "Requested"

        access_token_list = {}
        product_id_list = event.get("product_id_list", None)
        if product_id_list is not None and len(product_id_list) != 0:
            product_id_str = ",".join(product_id_list)
            products_cursor.execute(
                "SELECT mp.product_id,sp.snapdealp_portal_sku,sp.snapdealp_portal_unique_id,sp.snapdealp_listing_id,sp.snapdealp_current_selling_price,sp.snapdealp_upload_selling_price,mp.product_mrp,sp.snapdealp_account_id,sp.snapdealp_min_break_even_sp,sp.snapdealp_max_break_even_sp,sp.snapdealp_price_rule,sp.snapdealp_competitor_lowest_price,mp.product_length,mp.product_breath,mp.product_width,mp.product_weight,mp.min_payout_value,mp.max_payout_value,mp.hsn_code_id_id FROM snapdealp_snapdealpproducts as sp left join master_masterproduct as mp on sp.product_id = mp.product_id where sp.product_id in (" + product_id_str + ")")
        else:
            pass

            # products_cursor.execute(
            #     "SELECT mp.product_id,sp.snapdealp_portal_sku,sp.snapdealp_portal_unique_id,sp.snapdealp_listing_id,sp.snapdealp_current_selling_price,sp.snapdealp_upload_selling_price,mp.product_mrp,sp.snapdealp_account_id,sp.snapdealp_min_break_even_sp,sp.snapdealp_max_break_even_sp,sp.snapdealp_price_rule,sp.snapdealp_competitor_lowest_price,mp.product_length,mp.product_breath,mp.product_width,mp.product_weight,mp.min_payout_value,mp.max_payout_value,mp.hsn_code_id_id FROM snapdealp_snapdealpproducts as sp left join master_masterproduct as mp on sp.product_id = mp.product_id where sp.snapdealp_current_selling_price != sp.snapdealp_upload_selling_price limit 1500")

        snapdeal_list = products_cursor.fetchall()

        message1 = len(snapdeal_list)

        if len(snapdeal_list) != 0:

            products_cursor.execute(
                "Select account_id,authentication_attribute_values from master_portalaccountdetails where portal_id_id = " + str(
                    SNAPDEAL_PORTAL_ID) + " and account_id = 11")
            portal_accounts = products_cursor.fetchall()
            if len(portal_accounts):
                for portal_account in portal_accounts:

                    for item in portal_account[1]:
                        if item['name'] == 'xsellerauthztoken':
                            X_Seller_AuthZ_Token = item['base_url']
                            print("X_Seller_AuthZ_Token====", X_Seller_AuthZ_Token)
                            break

            url = "https://apigateway.snapdeal.com/seller-api/products/price"
            # 0--product_id,
            # 1--flipkart_portal_sku,
            # 2--flipkart_portal_unique_id,
            # 3--flipkart_listing_id,
            # 4--flipkart_current_selling_price,
            # 5--flipkart_upload_selling_price,
            # 6--product_mrp
            # 7--flipkart_account_id
            # 8-->fp.flipkart_min_break_even_sp,
            # 9-->fp.flipkart_max_break_even_sp,
            # 10-->fp.flipkart_price_rule,
            # 11-->fp.flipkart_competitor_lowest_price,
            # 12-->mp.product_length,
            # 13-->mp.product_breath,
            # 14-->mp.product_width,
            # 15-->mp.product_weight,
            # 16-->mp.min_payout_value,
            # 17-->mp.max_payout_value,
            # 18-->mp.hsn_code_id_id
            if len(snapdeal_list) != 0:
                message = "Requeste Processing"
                for single_product in snapdeal_list:
                    product_id = single_product[0]
                    snapdeal_sku = single_product[1]
                    snapdeal_account_id = int(single_product[7])
                    # access_token_id = access_token_list.get(snapdeal_account_id, None)

                    logger.info("product_id-----{}".format(single_product[0]))
                    logger.info("snapdealp_portal_sku-----{}".format(single_product[1]))
                    logger.info("snapdealp_portal_unique_id-----{}".format(single_product[2]))
                    logger.info("snapdealp_listing_id-----{}".format(single_product[3]))
                    logger.info("snapdealp_current_selling_price-----{}".format(single_product[4]))
                    logger.info("snapdealp_upload_selling_price-----{}".format(single_product[5]))
                    logger.info("product_mrp-----{}".format(single_product[6]))
                    logger.info("snapdealp_account_id-----{}".format(single_product[7]))
                    # logger.info("access_token_id-----{}".format(access_token_id))

                    a = str(single_product[2])
                    b = str(int(single_product[6]))
                    c = str(int(single_product[5]))
                    payload = "\n\n[{\n\"supc\": \"" + a + "\",\n\"mrp\": \"" + b + "\",\n\"sellingPrice\": \"" + c + "\"\n}]"

                    try:
                        headers = {
                            'x-auth-token': "300db07f-85f7-33dd-936e-614801f89bee",
                            'x-seller-authz-token': X_Seller_AuthZ_Token,
                            'clientid': "197",
                            'Content-Type': 'application/json'
                        }
                        response = requests.request("POST", url, data=payload, headers=headers)
                        print("response===", response)

                        if response:
                            res = "sucess"
                            response_data = response.json()
                            print("response_data===>", response_data)
                            # if response_data[flipkart_sku]['status'] == 'SUCCESS':
                            res = "sucess"
                            ## Updating current Selling Price
                            query_write = "UPDATE snapdealp_snapdealpproducts SET snapdealp_current_selling_price=%s  WHERE product_id=" + str(
                                product_id)
                            value_listings = [single_product[5]]
                            products_cursor.execute(query_write, value_listings)
                            conn_products.commit()
                            total_success += 1
                            print("{} price update {}".format(snapdeal_sku, res))
                            logger.info("{} price update {}".format(snapdeal_sku, res))
                            mongodb_insert = {"portal_id": SNAPDEAL_PORTAL_ID,
                                              "product_id": single_product[0],
                                              "portal_unique_id": single_product[2],
                                              "portal_listing_id": single_product[3],
                                              "min_selling_price": single_product[8],
                                              "max_selling_price": single_product[9],
                                              "price_rule": single_product[10],
                                              "product_length": single_product[12],
                                              "product_breath": single_product[13],
                                              "product_width": single_product[14],
                                              "product_weight": single_product[15],
                                              "current_selling_price": single_product[4],
                                              "upload_selling_price": single_product[5],
                                              "competitor_lowest_price": single_product[11],
                                              "min_payout_value": single_product[16],
                                              "max_payout_value": single_product[17],
                                              "account_id": single_product[7],
                                              "hsn_code_id": single_product[18],
                                              "created_at": datetime.fromisoformat(str(datetime.now()))}
                            price_update_table.insert_one(mongodb_insert)


                        # elif response_data[flipkart_sku]['status'] == 'FAILURE':
                        # 	res = ""
                        # 	errors_res = response_data.get(flipkart_sku,{}).get('errors',[])

                        # 	total_fail +=1
                        # 	attribute_errors = response_data.get(flipkart_sku,{}).get('attribute_errors',[])
                        # 	if len(errors_res)!=0:
                        # 		res = errors_res[0].get('description','No response')
                        # 	if len(attribute_errors) !=0:
                        # 		res = attribute_errors[0].get('description','No response')

                        # 	print("{} price update errors {}".format(flipkart_sku,res))
                        # 	logger.info("{} price update errors {}".format(flipkart_sku,res))
                        # 	writer.writerow({'product_id':single_product[0],'flipkart_portal_sku':single_product[1],'Res':res})

                        # 	### For Dummy Request Only
                        # 	# query_write = "UPDATE flipkart_flipkartproducts SET flipkart_current_selling_price=%s  WHERE product_id="+str(product_id)
                        # 	# value_listings = [single_product[5]]
                        # 	# products_cursor.execute(query_write, value_listings)
                        # 	# conn_products.commit()

                        # else:
                        # 	total_fail +=1
                        # 	print("Some Thing went Wrong---{}".format(response_data))
                        # 	logger.info("Some Thing went Wrong---{}".format(response_data))
                        else:
                            pass

                    except Exception as loopExcep:
                        message = "Exception--" + str(loopExcep)
                        print("Loop Exception--", message)

    except Exception as e:
        statusCode = 400
        message = "Exception--" + str(e)
        print(message)
    finally:
        conn_products.close()
        products_cursor.close()
        file_obj.close()

        file_to = '/buymore2/price_update/snapdeal/' + file_name
        dbx = dropbox.Dropbox(access_token)

        with open(file_path, 'rb') as f:
            data = dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

        return {'statusCode': 200, 'product_count': message1}
