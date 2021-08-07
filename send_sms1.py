import json

import http.client as ht

conn = ht.HTTPConnection("api.msg91.com")


def lambda_handler(event, context):
    # print("event==")
    # print(event)
    body = json.loads(event['body'])
    # print("body===")
    # print(body)
    message = body["message"]
    # print(message)
    to = body["to"]
    # print(to)
    num1 = []
    for e in map(str, to):
        num1.append(e)

    # print(num1)
    num = str(num1)
    number = num.replace("'", '"')
    # print("number==")
    # print(number)
    payload = '''{
    "sender": "BUYMOR",
    "route": "4",
    "country": "91",
    "sms":[
      {
    "message":''' + '"' + message + '"' + ''',
    "to":''' + number + '''
      }
     ]
    }'''
    print(payload)
    headers = {
        'authkey': "192268ArIqtNQVr5a548512a123b",
        'content-type': "application/json"
    }
    conn.request("POST", "/api/v2/sendsms?", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    print("SMS sent")
    return {
        'statusCode': 200,
        'message': 'SMS Sucessfully Sent'
    }

