# import os
import dropbox
import csv
import datetime
import pandas as pd
import requests
import psycopg2
# from sqlalchemy import create_engine
import json
def db_credential(db_name,typ):
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
    print(response,type(response))
    if status == True:
        return response['db_detail'][typ]
    else:
        return
db_creds=db_credential('postgres','db_detail_for_psycopg2')
# db_creds={"endPoint":"buymore-dev-aurora.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com","userName":"postgres","passWord":"r2DfZEyyNL2VLfg2"}
rds_host = db_creds['endPoint']
name = db_creds['userName']
password = db_creds['passWord']

def lambda_handler(event,context):
    if 'vendor_id' in event:
        vendor_id=str(event['vendor_id'])
    else:
        vendor_id=None
    if 'year' in event:
        opening_balance=str(event['year'])
    else:
        opening_balance=None
    if vendor_id is None or vendor_id=='0' or opening_balance is None:
        return {'message': 'vendor_id or year is mandatory'}
    if (not vendor_id.isnumeric()):
        return {'message': 'vendor_id should be numeric'}
    if (not opening_balance.isnumeric()):
        return {'message': 'year should be numeric'}
    if len(opening_balance)>2:
        opening_balance=opening_balance[-2:]
    if opening_balance not in ['20','21','22']:
        return {'message': 'year '+opening_balance +' not in table'}
    # vendor_id = 71
    # opening_balance = 20
    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    def get_po_from_amz_tbl(bsku):
        bsku = bsku.replace("'", "''")
        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "SELECT max(amazon_purchase_order_value)*1.18 FROM amazon_amazonproducts ap,master_masterproduct mp WHERE ap.product_id=mp.product_id AND buymore_sku='"+bsku+"'"
        cur = conn.cursor()
        cur.execute(qry)
        r1 = cur.fetchall()
        qry = "SELECT max(flipkart_purchase_order_value)*1.18 FROM flipkart_flipkartproducts fp,master_masterproduct mp WHERE fp.product_id=mp.product_id AND buymore_sku='"+bsku+"'"
        cur.execute(qry)
        r2=cur.fetchall()
        cur.close()
        conn.close()
        rv=0
        if r1[0][0]==None and r2[0][0]==None:
            rv=0
        elif r1[0][0] == None:
            rv=r2[0][0]
        elif r2[0][0]== None:
            rv=r1[0][0]
        elif r1[0][0]>r2[0][0]:
            rv=r1[0][0]
        else:
            rv=r2[0][0]
        return rv
    def get_soh(vid):
        url = "https://app.sellerbuymore.com/wms/vendor_stock_data/?vendor_id=" + vid
        payload = "{\r\n  \"vendor_id\": " + vid + "\r\n}"
        headers = {
            'Content-Type': 'application/json'
        }
        #print(payload)
        try:
            response = json.loads(requests.request("GET", url, headers=headers, data=payload).text)
        except Exception as e:
            #print(e)
            response = {str(e): 0}
        # response={"link":""}
        if "link" in response:
            file_name = 'Vendor_BuyMoreStock_' + str(vid) + '.csv'
            file_from = '/buymore2/bin_reco/csv_export/' + file_name
            file_to = '/tmp/' + file_name
            dbx = dropbox.Dropbox(access_token)
            dbx.files_download_to_file(download_path=file_to, path=file_from)
            with open(file_to, 'r', newline='') as f:
                soh = list(csv.reader(f))[1:]
            sohdict = {}
            for s in soh:
                if s[0] not in sohdict:
                    sohdict[s[0]] = int(s[1]) + int(s[2]) + int(s[3]) + int(s[4])
                else:
                    sohdict[s[0]] += int(s[1]) + int(s[2]) + int(s[3]) + int(s[4])

            db_name = "products"
            conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
            cur = conn.cursor()
            qry = "SELECT ms.buymore_sku,SUM(id.basic_price+id.net_gst)/SUM(id.quantity) FROM purchase_invoice_purchaseinvoices ip,purchase_invoice_purchaseskudetails id,master_masterproduct ms WHERE ip.purchase_invoice_id=id.purchase_invoice_id_id AND id.product_id=ms.product_id AND id.quantity>0 AND vendor_id=" + str(
                vid) + " GROUP BY ms.buymore_sku"  # AND invoice_date>='2020-10-01 00:00:00+00' AND invoice_date<='" + str(td) +
            #print(qry)
            cur.execute(qry)  #
            avg_billing = cur.fetchall()
            cur.close()
            conn.close()
            avg_billing_dict = {}
            ttavg = 0
            for a in avg_billing:
                if a[0] not in avg_billing_dict:
                    avg_billing_dict[a[0]] = float(a[1])
            with open('/tmp/soh_'+vid+'.csv','w',newline='') as f:
                wr=csv.writer(f)
                wr.writerow(['buymore_sku','quantity','average_billing_price_per_piece','average_billing_price_for_total_quantity'])
                for s in sohdict:
                    if s in avg_billing_dict:
                        ttavg += sohdict[s] * avg_billing_dict[s]
                        #print(s, sohdict[s] * avg_billing_dict[s], ttavg)
                        wr.writerow([s,sohdict[s],avg_billing_dict[s],sohdict[s] * avg_billing_dict[s]])
                    else:
                        poa=get_po_from_amz_tbl(s)
                        ttavg += sohdict[s] * poa
                        wr.writerow([s, sohdict[s], poa,sohdict[s]*poa])
                        # print(s, sohdict[s], 'not fnd')
            file_to='/buymore2/LEDGERS/soh_'+vid+'.csv'
            with open('/tmp/soh_'+vid+'.csv', 'rb') as f:
                read_data = f.read()
                data = dbx.files_upload(read_data, file_to, mode=dropbox.files.WriteMode.overwrite)
                # pass
            return ttavg
        else:
            return 0

    def getpayable(vid):
        td=datetime.datetime.utcnow()+datetime.timedelta(hours=5,minutes=30)+datetime.timedelta(days=60)
        url = "https://app.sellerbuymore.com/wms/payments_data/"
        payload = "{\r\n  \"vendor_id\" : \"" + vid + "\",\r\n  \"date\" : \"2020-10-01," + str(td)[:10] + "\"\r\n}"
        headers = {
            'Content-Type': 'application/json'
        }
        # print(payload)
        try:
            response = json.loads(requests.request("POST", url, headers=headers, data=payload).text)
            # print(response)
        except Exception as e:
            # print(e)
            response = {str(e): 0}
        if "data" in response:
            # print(response["data"][0]["payable_amount"])
            return response["data"][0]["payable_amount"]
        else:
            return 0

    startdate='20'+str(opening_balance)+'-04-01'
    enddate='20'+str(int(opening_balance)+1)+'-03-31'

    col1='opening_balance_'+str(opening_balance)
    # print(col1)
    db_name = "products"
    conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur = conn.cursor()
    # engine = create_engine(
    #     'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/products')
    query = 'select vendor_id,opening_balance_'+str(opening_balance)+ ' from purchase_invoice_openingbalance where vendor_id ='+str(vendor_id)
    #print("query==",query)
    cur.execute(query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    if len(result)==0:
        result=[[int(vendor_id),0]]
    temp_res=[]
    for r in result:
        if r[1]==None:
            temp_res.append([r[0],0])
        else:
            temp_res.append(r)
    result=temp_res
    with open('/tmp/a1.csv','w',newline='') as f:
        wr=csv.writer(f)
        wr.writerow(['vendor_id','opening_balance_'+str(opening_balance)])
        wr.writerows(result)
    # sql = pd.read_sql(query, engine)
    # print("sql2", sql)
    # sql.to_csv('E:/a1.csv', index=False)
    report = pd.DataFrame()
    report['date'] = ''
    report['reason'] = ''
    report['note'] = ''
    report['credit'] = ''
    report['debit'] = ''
    report.to_csv('/tmp/a2.csv', index=False)
    data1=pd.read_csv('/tmp/a1.csv')
    #print("data1==>",data1)
    val=int(data1['opening_balance_'+opening_balance])
    #print(val)
    if val > 0:
        report['debit']=abs(data1['opening_balance_'+opening_balance])
        report['reason'] = 'Opening balance'
        report['date'] = '20'+str(opening_balance)+'-04-01'
    else:
        report['credit'] = abs(data1['opening_balance_'+opening_balance])
        report['reason'] = 'Opening balance'
        report['date'] = '20'+str(opening_balance)+'-04-01'


    report.to_csv('/tmp/a4.csv', index=False)
    ###till opening balance
    ###purchase invoice
    conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur = conn.cursor()
    query1='select purchase_invoice_id,invoice_date,purchase_invoice_purchaseskudetails.basic_price,purchase_invoice_purchaseskudetails.net_gst from purchase_invoice_purchaseinvoices join purchase_invoice_purchaseskudetails on purchase_invoice_purchaseinvoices.purchase_invoice_id = purchase_invoice_purchaseskudetails.purchase_invoice_id_id where vendor_id ='+str(vendor_id)+' AND date(invoice_date)>=\''+startdate+'\' AND date(invoice_date)<=\''+enddate+'\''
    #print("query1==", query1)
    cur.execute(query1)
    result = cur.fetchall()
    cur.close()
    conn.close()
    with open('/tmp/a5.csv','w',newline='') as f:
        wr=csv.writer(f)
        wr.writerow(['purchase_invoice_id','invoice_date','basic_price','net_gst'])
        wr.writerows(result)
    # sql1 = pd.read_sql(query1, engine)
    # sql1.to_csv('E:/a5.csv', index=False)
    b2 = pd.read_csv('/tmp/a5.csv',usecols=['purchase_invoice_id','invoice_date'],
                     low_memory=False)
    b3 = b2.drop_duplicates(keep='first')
    b3.to_csv('/tmp/a15.csv', index=False)
    b4 = pd.read_csv('/tmp/a5.csv', usecols=['purchase_invoice_id','basic_price','net_gst'],
                     low_memory=False)
    d=b4.groupby('purchase_invoice_id').agg('sum')

    d.to_csv('/tmp/a6.csv')
    d["basic_price+net_gst"] = d[["basic_price","net_gst"]].sum(axis=1)
    decimals = 4
    d['basic_price+net_gst'] = d['basic_price+net_gst'].apply(lambda x: round(x, decimals))
    d['debit']=''
    d['credit']=''
    d.to_csv('/tmp/a7.csv')
    p_final = pd.read_csv('/tmp/a7.csv', usecols=['purchase_invoice_id','basic_price+net_gst','debit','credit'],
                     low_memory=False)
    negative = p_final[(p_final['basic_price+net_gst'] < 0)]
    negative['debit'] = abs(negative['basic_price+net_gst'])

    negative.to_csv('/tmp/a16.csv', index=False)
    negative_amount = pd.read_csv('/tmp/a16.csv', usecols=['purchase_invoice_id','debit','credit'],
                          low_memory=False)


    positive = p_final[(p_final['basic_price+net_gst'] > 0)]
    positive['credit'] = positive['basic_price+net_gst']
    positive.to_csv('/tmp/a17.csv', index=False)
    positive_amount = pd.read_csv('/tmp/a17.csv', usecols=['purchase_invoice_id', 'debit', 'credit'],
                                  low_memory=False)
    positive_amount.to_csv('/tmp/a18.csv',index=False)

    negative_amount.to_csv('/tmp/a18.csv', mode='a', index=False, header=False)
    df11 = pd.read_csv("/tmp/a15.csv")
    df12 = pd.read_csv("/tmp/a18.csv")
    df22 = pd.merge(left=df12, right=df11, left_on='purchase_invoice_id', right_on='purchase_invoice_id')
    df22['reason'] =df22['purchase_invoice_id']
    df22['date'] = df22['invoice_date'].str[:10]
    df22['note'] = 'purchase_invoice'
    # dfST['new_date_column'] = dfST['timestamp'].dt.date
    df22.debit.abs()
    df22.to_csv('/tmp/a19.csv', index=False)



    df221 = df22[['date','reason','note','credit','debit']]
    df221.debit.abs()
    df221.credit.abs()
    df221.to_csv('/tmp/a20.csv', index=False)
    purchase_invoice_data = pd.read_csv("/tmp/a20.csv")



    ####payment section
    db_name = "vendors"
    # engine1=create_engine(
    #     'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/vendors')
    query2='select transaction_id,date_to,amount from api_master_payment where transaction_status =1 and amount>0 and vendor_id ='+str(vendor_id)+' AND date(date_to)>=\''+startdate+'\' AND date(date_to)<=\''+enddate+'\''
    # sqlp = pd.read_sql(query2, engine1)
    # sqlp.to_csv('E:/a10.csv', index=False)
    conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    cur = conn.cursor()
    cur.execute(query2)
    result = cur.fetchall()
    cur.close()
    conn.close()

    with open('/tmp/a10.csv','w',newline='') as f:
        wr=csv.writer(f)
        wr.writerow(['transaction_id','date_to','amount'])
        wr.writerows(result)
    p1 = pd.read_csv('/tmp/a10.csv')
    p1['date'] = ''
    p1['reason'] = ''
    p1['credit'] = ''
    p1['debit'] = ''
    p1.to_csv('/tmp/a11.csv', index=False)
    p2 = pd.read_csv('/tmp/a11.csv')
    p2['date']=p2['date_to']
    p2['reason']=p2['transaction_id']
    p2['debit'] = p2['amount']
    p2['note'] = 'payments'
    p2.to_csv('/tmp/a12.csv', index=False)
    p4 = pd.read_csv('/tmp/a12.csv',usecols=['date','reason','note','credit','debit'],
                                                  low_memory=False)
    p4.to_csv('/tmp/a14.csv', index=False)
    p5 = p4[['date', 'reason', 'note', 'credit', 'debit']]
    p5.to_csv('/tmp/a15.csv', index=False)
    payment_data = pd.read_csv('/tmp/a15.csv')


    ##adding all (3)data together
    purchase_invoice_data.to_csv('/tmp/a4.csv', mode='a', index=False, header=False)
    payment_data.to_csv('/tmp/a4.csv', mode='a', index=False, header=False)
    final_report=pd.read_csv('/tmp/a4.csv')
    final_report.debit.abs()
    final_report.credit.abs()
    final_report.to_csv('/tmp/a21.csv',index=False)

    credit_total = final_report['credit'].sum()
    #print("credit_total==",credit_total)
    debit_total = final_report['debit'].sum()
    #print("debit_total==", debit_total)
    #print("sucessfull===")
    test = pd.DataFrame(columns=['date','reason','note','credit','debit'])
    test.to_csv('/tmp/a22.csv',index=False)
    cal = pd.read_csv('/tmp/a22.csv')
    if credit_total<=debit_total:
        cal.loc[1, 'debit'] = debit_total-credit_total
        cal.loc[1, 'reason'] = 'Closing_balance'#'Net_Credit'
        cal.loc[2, 'debit'] = get_soh(vendor_id)
        cal.loc[2, 'reason'] = 'Stock_value'
        cal.loc[3, 'debit'] = getpayable(vendor_id)
        cal.loc[3, 'reason'] = 'Reserved_payment'

    else:
        cal.loc[1, 'credit'] = credit_total-debit_total
        cal.loc[1, 'reason'] = 'Closing_balance'#Net_Debit
        cal.loc[2, 'credit'] = get_soh(vendor_id)
        cal.loc[2, 'reason'] = 'Stock_value'
        cal.loc[3, 'credit'] = getpayable(vendor_id)
        cal.loc[3, 'reason'] = 'Reserved_payment'

    #print("test==",test)
    cal.to_csv('/tmp/a24.csv',index=False)
    final_cal=pd.read_csv('/tmp/a24.csv')
    final_cal.to_csv('/tmp/a21.csv', mode='a', index=False, header=False)
    result_file= pd.read_csv('/tmp/a21.csv').sort_values(by=['date'])
    result_file.to_csv('/tmp/payment_report_'+str(vendor_id)+'.csv',index=False)
    #print('file generated')
    #print('writing to dropbox')
    dbx = dropbox.Dropbox(access_token)
    file_from='/tmp/payment_report_'+str(vendor_id)+'.csv'
    file_to='/buymore2/LEDGERS/'+file_from.split('/')[-1]
    with open(file_from, 'rb') as f:
        read_data = f.read()
        data = dbx.files_upload(read_data, file_to, mode=dropbox.files.WriteMode.overwrite)
        link = dbx.files_get_temporary_link(data.path_display).link
    #print('written to dropbox')
    print('run succed')
    return {'message': 'file generated','link':link}
    # engine.dispose()

