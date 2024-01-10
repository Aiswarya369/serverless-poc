import requests
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

url = "https://rt2tajh0a9.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4274002"
headers = {"Content-Type": "application/json"}

for i in range(5, 10):
    req = {
        "site": "AJ0000000{}".format(i),
        "start_datetime": "2024-01-10T03:20:00+00:00",
        "end_datetime": "2024-01-10T03:25:00+00:00",
        "status": "ON",
        "switch_addresses": "LG02210255{}".format(i),
        "group_id": "G4",
    }
    payload = json.dumps(req)
    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.status_code, response.reason)

