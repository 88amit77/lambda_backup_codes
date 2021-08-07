import http.client


def lambda_handler(event, context):
    conn = http.client.HTTPSConnection("api.msg91.com")
    headers = {'content-type': "application/json"}
    auth_key = '192268ArIqtNQVr5a548512'
    sms_type = {
        'po_verify': '5fdaec5174670927fb3795c88',
        'pricing_update': '5fdaecd985ac9f1d76734e9e8776765',
        'sign_off': '6093cd57e0d2104cae5a33054rr',
        'WMS': '60dc283e2874767d7f6695d84543e'
    }
    template_type = event['template']
    template_id = sms_type[template_type]
    phone_number = event['mobile_number']
    conn.request("GET", "/api/v5/otp?authkey=" + auth_key + "&template_id=" + template_id + "&mobile=" + str(
        phone_number) + "&otp_expiry=360", headers=headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

    return {
        'data': data.decode("utf-8")
    }