import requests
import json

url = "https://rt2tajh0a9.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(1, 2):
    req = {
        "site": "CNTEST2000XXXXX",
        "start_datetime": "2023-12-23T03:00:00+00:00",
        "end_datetime": "2023-12-23T04:15:00+00:00",
        "status": "ON",
        "switch_addresses": "LG021812235",
    }

    payload = json.dumps(req)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(json.dumps(req, indent=4))
    print(response.status_code, response.reason)
    print(json.dumps(response.json(), indent=4))
